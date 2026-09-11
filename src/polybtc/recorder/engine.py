"""Bucle de 1 Hz del grabador: lee los feeds, calcula el modelo y lo deja en disco.

Es el único proceso que escribe. Cada segundo produce una fila con el estado completo
—precios, insumos del modelo, libro, edades y banderas de halt— y reescribe el heartbeat
en `state.db`. No coloca órdenes, no calcula PnL y no tiene wallet: la Fase 0 es
observación, y un panel de PnL a cero solo invita a saltársela (§6).

Dos detalles del ciclo de vida que no son evidentes y sí importan para 14 días seguidos:

- **La suscripción al libro se reinicia con cada época.** Los token ids rotan (§8.1), así
  que el feed de `book` se cancela y se vuelve a lanzar con los tokens nuevos, y los libros
  viejos se olvidan de `FeedState` para que su edad no quede colgando.
- **El apagado es ordenado.** `SIGTERM` vuelca el buffer y cierra la hora en curso
  convirtiéndola a Parquet, que es lo que pide el criterio de salida de este sprint.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import signal
import sys
from datetime import UTC, datetime

from polybtc.config import Settings
from polybtc.feeds import book, rtds, spot
from polybtc.feeds.discovery import Epoch
from polybtc.feeds.state import (
    FEED_BINANCE_DIRECT,
    FEED_BINANCE_RELAY,
    FEED_CHAINLINK,
    FEED_TWAP30,
    FEED_TWAP60,
    FeedState,
    Quote,
    book_feed,
)
from polybtc.model.basis import BasisEstimator
from polybtc.model.fair import fair_value
from polybtc.model.vol import EwmaVol, deannualize
from polybtc.recorder import halts
from polybtc.recorder.epochs import EpochTracker
from polybtc.recorder.schema import ModelPoint, Sample, f, window_suffix
from polybtc.recorder.sink import ParquetSink, recover_orphans
from polybtc.recorder.status import StatusStore

REQUIRED_FEEDS: tuple[tuple[str, str], ...] = (
    (FEED_CHAINLINK, "chainlink"),
    (FEED_TWAP30, "twap30"),
    (FEED_TWAP60, "twap60"),
    (FEED_BINANCE_DIRECT, "binance"),
)
"""Pares `(clave de staleness, etiqueta)` de los feeds sin los cuales no habría fair.

`binance_relay` queda fuera a propósito: es el cross-check de §2.1.b, no un insumo — su
ausencia no ciega el modelo. La etiqueta existe porque la clave de un libro lleva el token
id dentro y cambia con cada época (ver `halts.evaluate`).
"""


class Recorder:
    """Une feeds, modelo, época y disco. Un solo event loop, un solo escritor."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._state = FeedState(staleness_s=settings.staleness_s)
        self._basis = BasisEstimator(
            half_life_s=settings.basis_half_life_s,
            clamp=settings.basis_clamp,
            min_samples=settings.basis_min_samples,
        )
        self._vol = EwmaVol(
            half_life_s=settings.vol_half_life_s,
            floor_s=deannualize(settings.vol_floor_annual),
            ceiling_s=deannualize(settings.vol_ceiling_annual),
        )
        self._windows = tuple(settings.twap_window_hypotheses_s)
        self._store = StatusStore(settings.state_db_path)
        self._sink = ParquetSink(settings.data_dir, self._windows)
        self._tracker = EpochTracker(settings, self._store, on_epoch_change=self._on_epoch_change)

        self._book_task: asyncio.Task[None] | None = None
        self._book_tokens: tuple[str, ...] = ()
        self._started_at = datetime.now(UTC)
        self._ticks = 0
        self._last_flush_at: datetime | None = None
        self._pending_epoch: Epoch | None = None
        self._epoch_dirty = False

    # -- ciclo de vida -----------------------------------------------------------------

    async def run(self) -> None:
        """Arranca todo y graba hasta que llegue una señal de parada."""
        recovered = recover_orphans(self._settings.data_dir)
        if recovered:
            # Restos de una muerte no ordenada, ya convertidos a Parquet legible.
            self._note(f"recuperados {len(recovered)} archivo(s) de una corrida anterior")

        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, stop.set)

        tasks = [
            asyncio.create_task(rtds.run(self._state, self._settings), name="rtds"),
            asyncio.create_task(spot.run(self._state, self._settings), name="spot"),
            asyncio.create_task(self._tracker.run_discovery(), name="discovery"),
            asyncio.create_task(self._tracker.run_resolutions(), name="resolutions"),
        ]
        try:
            await self._tick_loop(stop)
        finally:
            for task in tasks:
                task.cancel()
            await self._stop_book()
            await asyncio.gather(*tasks, return_exceptions=True)
            # El orden importa: primero el disco de datos, después el estado vivo. Si algo
            # fallara aquí, lo que tiene que estar completo es la Fase 0, no el heartbeat.
            self._sink.close()
            self._store.close()

    async def _tick_loop(self, stop: asyncio.Event) -> None:
        """Un tick por segundo, **alineado al segundo de reloj**, no cada 1,0 s.

        Dormir un segundo fijo acumula el tiempo de trabajo de cada vuelta: unos pocos
        milisegundos por tick son ~18 filas menos por hora y, peor, una rejilla que se
        desliza. §3.4 pide una rejilla temporal FIJA de 1 s para σ, y la frontera de las
        épocas cae en segundos enteros: alinearse a ellos hace que el strike se observe en
        el segundo exacto de la apertura en vez de a un azar de hasta un segundo.
        """
        period = self._settings.recorder_tick_s
        while not stop.is_set():
            self.tick(datetime.now(UTC))
            await self._apply_epoch_change()
            delay = period - (datetime.now(UTC).timestamp() % period)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=delay)

    # -- un segundo --------------------------------------------------------------------

    def tick(self, now: datetime) -> Sample:
        """Produce y encola la fila de este segundo. Sin red y sin `await`: es testeable.

        Todo el I/O del grabador vive en `run`; esta función solo lee la pizarra de feeds y
        escribe en disco a través del sink. Es la misma disciplina que §4 exige de la capa
        de estrategia, y por el mismo motivo: lo que no se puede ejecutar sin red no se
        puede probar.
        """
        self._ticks += 1
        sample = self._build_sample(now)
        self._sink.append(sample)

        if self._should_flush(now):
            self._sink.flush()
            self._last_flush_at = now

        self._write_status(now, sample)
        return sample

    def _build_sample(self, now: datetime) -> Sample:
        state = self._state
        chainlink = f(state.prices.get(FEED_CHAINLINK))
        twap30 = f(state.prices.get(FEED_TWAP30))
        twap60 = f(state.prices.get(FEED_TWAP60))
        binance = f(state.prices.get(FEED_BINANCE_DIRECT))
        relay = f(state.prices.get(FEED_BINANCE_RELAY))

        self._update_estimators(now, chainlink, binance)
        self._tracker.observe(now, chainlink, twap30, twap60)

        spot_oracle = self._spot_in_oracle_units(binance)
        epoch = self._tracker.current
        strike = self._tracker.strike
        tau_s = None if epoch is None else epoch.tau_s(now)

        up, down = self._quotes(epoch)
        models = self._evaluate_models(now, spot_oracle, strike.value, tau_s)

        flags = halts.evaluate(
            state,
            self._required_feeds(epoch),
            basis_ready=self._basis.ready,
            basis_clamped=self._basis.last_observation_clamped,
            sigma_ready=self._vol.ready,
            sigma_in_band=self._vol.in_band,
            has_market=epoch is not None,
            has_strike=strike.usable,
        )

        return Sample(
            ts=now,
            chainlink=chainlink,
            twap30=twap30,
            twap60=twap60,
            binance_mid=binance,
            binance_relay=relay,
            basis_log=self._basis.log_basis if self._basis.samples else None,
            basis_usd=self._basis_usd(binance),
            basis_ready=self._basis.ready,
            basis_samples=self._basis.samples,
            basis_clamped=self._basis.last_observation_clamped,
            spot_oracle=spot_oracle,
            sigma_s=self._vol.sigma_s if self._vol.samples else None,
            sigma_annual=self._vol.sigma_annual if self._vol.samples else None,
            sigma_ready=self._vol.ready,
            sigma_in_band=self._vol.in_band,
            sigma_samples=self._vol.samples,
            epoch_slug=None if epoch is None else epoch.slug,
            epoch_end=None if epoch is None else epoch.end,
            tau_s=tau_s,
            strike=strike.value,
            strike_source=strike.source,
            up_bid=None if up is None else f(up.bid),
            up_ask=None if up is None else f(up.ask),
            down_bid=None if down is None else f(down.bid),
            down_ask=None if down is None else f(down.ask),
            up_tick=None if epoch is None else f(state.tick_size(epoch.up_token)),
            down_tick=None if epoch is None else f(state.tick_size(epoch.down_token)),
            models=models,
            halted=flags.halted,
            halt_reasons=flags.joined(),
            age_chainlink=state.age_s(FEED_CHAINLINK),
            age_twap30=state.age_s(FEED_TWAP30),
            age_twap60=state.age_s(FEED_TWAP60),
            age_binance_direct=state.age_s(FEED_BINANCE_DIRECT),
            age_binance_relay=state.age_s(FEED_BINANCE_RELAY),
            age_book_up=None if epoch is None else state.age_s(book_feed(epoch.up_token)),
            age_book_down=None if epoch is None else state.age_s(book_feed(epoch.down_token)),
            reconnects=sum(state.reconnects.values()),
        )

    # -- modelo ------------------------------------------------------------------------

    def _update_estimators(
        self, now: datetime, chainlink: float | None, binance: float | None
    ) -> None:
        """Alimenta basis y σ, cada uno solo con datos que tienen derecho a moverlos.

        El basis exige que NINGUNO de los dos lados esté stale: mezclar un Chainlink de
        hace 5 s con un Binance de hace 20 ms no mide el basis, mide el rezago del TWAP.

        σ exige que el mid de Binance esté fresco por un motivo distinto: `EwmaVol` se
        re-ancla ante un hueco, pero si le pasáramos el último mid conocido durante una
        desconexión vería una serie plana y sesgaría σ a la baja justo después de un corte,
        que es cuando menos hay que fiarse de ella.
        """
        ts = now.timestamp()
        state = self._state
        if (
            chainlink is not None
            and binance is not None
            and not state.is_stale(FEED_CHAINLINK)
            and not state.is_stale(FEED_BINANCE_DIRECT)
        ):
            self._basis.update(ts, chainlink, binance)
        if binance is not None and not state.is_stale(FEED_BINANCE_DIRECT):
            self._vol.update(ts, binance)

    def _spot_in_oracle_units(self, binance: float | None) -> float | None:
        """Spot de Binance corregido a unidades del oráculo, o `None` si no se puede.

        `None` cuando el basis no ha convergido no es un hueco molesto: es la regla de §5.2
        —basis sin converger, no cotizar— expresada como dato. Un fair calculado sobre un
        basis a medias sería peor que ninguno, porque el sesgo entra entero en `ln(S/K)`.
        """
        if binance is None or not self._basis.ready:
            return None
        return self._basis.to_oracle(binance)

    def _basis_usd(self, binance: float | None) -> float | None:
        """El basis en dólares. En logs es ilegible y nadie nota que se está moviendo."""
        if binance is None or self._basis.samples == 0:
            return None
        return binance * (self._basis.basis_ratio - 1.0)

    def _evaluate_models(
        self,
        now: datetime,
        spot_oracle: float | None,
        strike: float | None,
        tau_s: float | None,
    ) -> dict[float, ModelPoint]:
        """Fair bajo CADA hipótesis de ventana de §8. Ver `Settings.twap_window_hypotheses_s`.

        Una hipótesis que no se puede evaluar se omite en vez de rellenarse: la columna
        queda nula y el análisis lo ve. Rellenar con el spot crudo cuando falta la media
        observada sería reintroducir las señales fantasma que §3.3 elimina.
        """
        if spot_oracle is None or strike is None or tau_s is None or not self._vol.ready:
            return {}

        sigma_s = self._vol.sigma_s
        points: dict[float, ModelPoint] = {}
        for window in self._windows:
            try:
                result = fair_value(
                    spot=spot_oracle,
                    strike=strike,
                    tau_s=tau_s,
                    window_s=window,
                    sigma_s=sigma_s,
                    observed_mean=self._tracker.observed_mean(window, now),
                )
            except ValueError:
                continue
            points[window] = ModelPoint(
                fair=result.fair,
                fair_band=result.fair_band,
                band_low=result.band_low,
                band_high=result.band_high,
                spot_eff=result.spot_eff,
                variance=result.variance,
                regime=str(result.regime),
                delta_per_dollar=result.delta_per_dollar,
            )
        return points

    # -- libro y época -----------------------------------------------------------------

    def _quotes(self, epoch: Epoch | None) -> tuple[Quote | None, Quote | None]:
        if epoch is None:
            return None, None
        return self._state.book(epoch.up_token), self._state.book(epoch.down_token)

    def _required_feeds(self, epoch: Epoch | None) -> tuple[tuple[str, str], ...]:
        """Los feeds exigidos incluyen el libro solo cuando hay un mercado que seguir."""
        if epoch is None:
            return REQUIRED_FEEDS
        return (
            *REQUIRED_FEEDS,
            (book_feed(epoch.up_token), "book_up"),
            (book_feed(epoch.down_token), "book_down"),
        )

    def _on_epoch_change(self, epoch: Epoch | None) -> None:
        """Callback del tracker. Solo apunta el cambio: reconectar es asíncrono."""
        self._pending_epoch = epoch
        self._epoch_dirty = True

    async def _apply_epoch_change(self) -> None:
        """Resuscribe el feed de libro a los tokens de la época nueva.

        Cancelar y relanzar la tarea en vez de mutar la suscripción viva: `book.run` lee
        los tokens al conectar, y una reconexión limpia es más fácil de razonar que un
        protocolo de resuscripción que la documentación no describe.
        """
        if not self._epoch_dirty:
            return
        self._epoch_dirty = False
        epoch = self._pending_epoch

        await self._stop_book()
        for token in self._book_tokens:
            self._state.forget_book(token)
        self._book_tokens = ()

        if epoch is None:
            return
        self._book_tokens = epoch.tokens()
        settings = self._settings.model_copy(update={"token_ids": self._book_tokens})
        self._book_task = asyncio.create_task(book.run(self._state, settings), name="book")

    async def _stop_book(self) -> None:
        if self._book_task is None:
            return
        self._book_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await self._book_task
        self._book_task = None

    # -- disco -------------------------------------------------------------------------

    def _should_flush(self, now: datetime) -> bool:
        if self._last_flush_at is None:
            return True
        return (now - self._last_flush_at).total_seconds() >= self._settings.recorder_flush_s

    def _write_status(self, now: datetime, sample: Sample) -> None:
        fairs = {
            window_suffix(window): {
                "fair": point.fair,
                "fair_band": point.fair_band,
                "band_low": point.band_low,
                "band_high": point.band_high,
                "regime": point.regime,
                "delta_per_dollar": point.delta_per_dollar,
            }
            for window, point in sample.models.items()
        }
        part = self._sink.current_part
        self._store.write_status(
            {
                "updated_at": now.isoformat(),
                "pid": os.getpid(),
                "started_at": self._started_at.isoformat(),
                "uptime_s": (now - self._started_at).total_seconds(),
                "ticks": self._ticks,
                "rows_written": self._sink.rows_written,
                "rows_pending": self._sink.pending,
                "last_flush_at": None
                if self._last_flush_at is None
                else self._last_flush_at.isoformat(),
                "current_part": None if part is None else str(part),
                "halted": sample.halted,
                "halt_reasons": sample.halt_reasons,
                "reconnects": sample.reconnects,
                "age_chainlink": sample.age_chainlink,
                "age_twap30": sample.age_twap30,
                "age_twap60": sample.age_twap60,
                "age_binance_direct": sample.age_binance_direct,
                "age_binance_relay": sample.age_binance_relay,
                "age_book_up": sample.age_book_up,
                "age_book_down": sample.age_book_down,
                "staleness_s": self._settings.staleness_s,
                "chainlink": sample.chainlink,
                "twap30": sample.twap30,
                "twap60": sample.twap60,
                "binance_mid": sample.binance_mid,
                "binance_relay": sample.binance_relay,
                "basis_usd": sample.basis_usd,
                "basis_log": sample.basis_log,
                "basis_ready": sample.basis_ready,
                "basis_samples": sample.basis_samples,
                "basis_clamped": sample.basis_clamped,
                "spot_oracle": sample.spot_oracle,
                "sigma_s": sample.sigma_s,
                "sigma_annual": sample.sigma_annual,
                "sigma_ready": sample.sigma_ready,
                "sigma_in_band": sample.sigma_in_band,
                "sigma_samples": sample.sigma_samples,
                "sigma_floor_annual": self._settings.vol_floor_annual,
                "sigma_ceiling_annual": self._settings.vol_ceiling_annual,
                "epoch_slug": sample.epoch_slug,
                "epoch_end": None if sample.epoch_end is None else sample.epoch_end.isoformat(),
                "tau_s": sample.tau_s,
                "strike": sample.strike,
                "strike_source": sample.strike_source,
                "up_bid": sample.up_bid,
                "up_ask": sample.up_ask,
                "down_bid": sample.down_bid,
                "down_ask": sample.down_ask,
                "up_tick": sample.up_tick,
                "down_tick": sample.down_tick,
                "fairs_json": _json(fairs),
                "windows_json": _json([window_suffix(w) for w in self._windows]),
            }
        )

    def _note(self, message: str) -> None:
        """Aviso por stderr. El grabador no habla más que esto.

        Su salida es `state.db` y los Parquet; quien mira es el dashboard. Bajo `systemd`
        (Sprint 4) stderr acaba en el journal del sistema, que es donde debe estar un aviso
        de arranque.
        """
        sys.stderr.write(f"polybtc.recorder: {message}\n")


def _json(value: object) -> str:
    return json.dumps(value, separators=(",", ":"))
