"""Seguimiento de la época viva: descubrirla, fijar su strike, y leer su desenlace.

Este módulo es el que convierte la Fase 0 en un experimento y no solo en una grabación.
`CLAUDE.md` §8 deja abierta la ventana TWAP del mercado y dice cómo cerrarla: «grabar
TWAP30/TWAP60 a 1 s junto a la frontera y la resolución de cada época, y ver qué hipótesis
predice los desenlaces». La frontera y la resolución salen de aquí.

Tres cosas que hay que hacer bien o el experimento no mide nada:

- **El strike es el precio en el instante de apertura**, no el precio cuando nos enteramos
  de que la época existe. §8.1 midió que el mercado de las 19:00 UTC no aparecía hasta las
  19:00:06: seis segundos de BTC son varios dólares, y a τ pequeño eso son céntimos de
  fair. Por eso se guarda un buffer de observaciones a 1 s y el strike se toma **hacia
  atrás**, del instante exacto de apertura.
- **Si no tenemos ese instante, no hay strike.** Al arrancar a mitad de una época no se
  inventa: se marca `missed` y esa época no produce fair. Lo que sí se conserva es la serie
  cruda completa, así que el análisis del Sprint 5 puede recalcular ese fair fuera de línea
  — lo único que se pierde es el fair en vivo de como mucho una época por reinicio.
- **La resolución llega tarde y muta una fila ya escrita.** Por eso las épocas viven en
  SQLite y no en Parquet, con un journal JSONL append-only al lado como registro inmutable.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from polybtc.config import Settings
from polybtc.feeds.discovery import (
    Epoch,
    current_of,
    fetch_epochs,
    fetch_market,
    resolved_outcome,
)
from polybtc.recorder.status import StatusStore

STRIKE_MAX_GAP_S = 2.0
"""Cuánto puede alejarse la observación usada como strike del instante de apertura.

Dos segundos no es una cifra cómoda elegida a ojo: con σ=60% anual, BTC recorre del orden
de $10 en dos segundos, que a τ=60 s son ~6 céntimos de fair según `delta_per_dollar`. Ya
es del orden del colchón de modelo de 3¢ (§3.5), así que más margen que este convertiría el
strike en la mayor fuente de error del experimento. Por encima: `missed`, y sin fair.
"""

SETTLE_MAX_GAP_S = 2.0
"""Lo mismo para los valores de cierre. Un cierre desalineado falsea la variable dependiente."""

BUFFER_SECONDS = 1800
"""Media hora de observaciones a 1 s. Cubre dos épocas de 15 minutos con holgura."""

JOURNAL_NAME = "epochs.jsonl"


@dataclass(frozen=True, slots=True)
class Observation:
    """Una lectura del oráculo a 1 s. Es de aquí de donde salen el strike y el cierre."""

    ts: datetime
    chainlink: float | None
    twap30: float | None
    twap60: float | None


@dataclass(frozen=True, slots=True)
class Strike:
    """Strike de una época y de dónde salió. `source` es un dato, no un adorno."""

    value: float | None
    source: str
    """`open` = observado en la apertura. `missed` = no había observación utilizable."""

    ts: datetime | None
    chainlink: float | None
    twap30: float | None
    twap60: float | None

    @property
    def usable(self) -> bool:
        return self.value is not None


MISSED = Strike(value=None, source="missed", ts=None, chainlink=None, twap30=None, twap60=None)


class EpochTracker:
    """Época viva, su strike y el estado de resolución de las que ya cerraron.

    Vive en el mismo event loop que el grabador. Las mutaciones ocurren sin `await` entre
    medias, así que no hace falta lock: el bucle de 1 Hz y las tareas de red nunca se
    entrelazan a mitad de una actualización.
    """

    def __init__(
        self,
        settings: Settings,
        store: StatusStore,
        *,
        on_epoch_change: Callable[[Epoch | None], None] | None = None,
    ) -> None:
        self._settings = settings
        self._store = store
        self._on_epoch_change = on_epoch_change
        self._observations: deque[Observation] = deque(maxlen=BUFFER_SECONDS)
        self._current: Epoch | None = None
        self._upcoming: list[Epoch] = []
        """Épocas ya conocidas que aún no han vencido. Las rellena `run_discovery`."""

        self._closed: set[str] = set()
        """Épocas ya cerradas por este proceso, para no re-adoptar la que acaba de vencer."""

        self._strike: Strike = MISSED
        self._journal = settings.data_dir / JOURNAL_NAME

    # -- estado ------------------------------------------------------------------------

    @property
    def current(self) -> Epoch | None:
        return self._current

    @property
    def strike(self) -> Strike:
        return self._strike

    def tau_s(self, now: datetime) -> float | None:
        return None if self._current is None else self._current.tau_s(now)

    # -- alimentación desde el bucle de 1 Hz -------------------------------------------

    def observe(
        self,
        ts: datetime,
        chainlink: float | None,
        twap30: float | None,
        twap60: float | None,
    ) -> None:
        """Registra la lectura de este segundo, y abre o cierra la época si toca.

        **El cambio de época lo decide este bucle de 1 Hz, no el sondeo de red.** Las
        épocas futuras ya existen con horas de antelación, así que la tarea de red solo
        rellena una lista y aquí se adopta la que corresponda en el segundo exacto en que
        empieza. Si esperásemos al siguiente sondeo, el strike se tomaría con el retraso de
        ese sondeo, se saldría de la tolerancia de `STRIKE_MAX_GAP_S` y la época entera se
        quedaría sin fair — cada vez.
        """
        self._observations.append(Observation(ts, chainlink, twap30, twap60))

        if self._current is not None and ts >= self._current.end:
            self._close_current(self._current)
        if self._current is None:
            upcoming = current_of(self._upcoming, ts)
            if upcoming is not None and upcoming.slug not in self._closed:
                self._adopt(upcoming)

    def observed_mean(self, window_s: float, now: datetime) -> float | None:
        """Media del oráculo sobre la parte YA TRANSCURRIDA de la ventana de liquidación.

        Es la `media_observada` de §3.3, y se calcula con el acumulador local a propósito,
        no con el feed TWAP de RTDS. No es una preferencia: son cantidades distintas. El
        TWAP de RTDS es una media **móvil** sobre `[t−w, t]`, mientras que lo que pide
        §3.3 es la media sobre `[T−w, t]`, el trozo de la ventana de liquidación que ya es
        un hecho consumado. Los dos intervalos solo coinciden en el instante `t = T`.
        Usar el feed móvil aquí metería una cantidad ajena justo en los últimos segundos,
        que es donde la gamma es máxima (§2.3) y donde equivocarse cuesta más.

        Además, la hipótesis `w=900` de §8 no tiene feed de RTDS que valga: no existe un
        stream de TWAP de 15 minutos. Sin acumulador local esa hipótesis no se puede ni
        evaluar, y es la mitad del experimento.
        """
        if self._current is None:
            return None
        window_start = self._current.end - timedelta(seconds=window_s)
        values = [
            o.chainlink
            for o in self._observations
            if o.chainlink is not None and window_start <= o.ts <= now
        ]
        return sum(values) / len(values) if values else None

    # -- tareas de red -----------------------------------------------------------------

    async def run_discovery(self) -> None:
        """Mantiene al día la lista de épocas próximas. No adopta ninguna: eso es `observe`.

        Los errores de red se tragan y se reintenta: un grabador que se cayera cada vez que
        la API tose no llegaría al día 2 de los 14. `CancelledError` hereda de
        `BaseException`, así que un apagado ordenado sí atraviesa este `suppress`.
        """
        while True:
            with contextlib.suppress(Exception):
                await self._discover_once()
            await asyncio.sleep(self._settings.epoch_discovery_poll_s)

    async def _discover_once(self) -> None:
        now = datetime.now(UTC)
        epochs = await fetch_epochs(
            self._settings.market_slug_prefix,
            epoch_seconds=self._settings.epoch_seconds,
            now=now,
        )
        if epochs:
            self._upcoming = [e for e in epochs if e.end > now]

    async def run_resolutions(self) -> None:
        """Pregunta por el desenlace de las épocas cerradas hasta obtenerlo o rendirse.

        Es la variable dependiente del experimento de §8: sin ella, las dos series de TWAP
        grabadas no se pueden contrastar contra nada.
        """
        while True:
            with contextlib.suppress(Exception):
                await self._resolve_pending()
            await asyncio.sleep(self._settings.epoch_resolution_poll_s)

    async def _resolve_pending(self) -> None:
        now = datetime.now(UTC)
        for row in self._store.pending_resolutions():
            slug = str(row["slug"])
            end = _parse_ts(row.get("epoch_end"))
            if end is None or end > now:
                continue
            attempts = int(row.get("resolution_attempts") or 0)
            if (now - end).total_seconds() > self._settings.epoch_resolution_deadline_s:
                self._record(
                    slug=slug,
                    resolved_outcome="abandoned",
                    resolved_at=_iso(now),
                    resolution_attempts=attempts,
                )
                continue

            outcome: str | None = None
            with contextlib.suppress(Exception):
                outcome = resolved_outcome(await fetch_market(slug))
            self._record(
                slug=slug,
                resolution_attempts=attempts + 1,
                **({"resolved_outcome": outcome, "resolved_at": _iso(now)} if outcome else {}),
            )

    # -- interno -----------------------------------------------------------------------

    def _adopt(self, epoch: Epoch) -> None:
        """Pasa a seguir una época nueva: fija su strike y avisa a quien la necesite."""
        if self._current is not None and self._current.slug != epoch.slug:
            self._close_current(self._current)

        self._current = epoch
        self._strike = self._strike_at(epoch.start)
        # Cuánto antes se creó el mercado. Medido: del orden de un día, no de segundos —
        # por eso `state.start_date` no sirve como instante de apertura (ver `discovery`).
        created_ahead = (
            None if epoch.created_at is None else (epoch.start - epoch.created_at).total_seconds()
        )
        self._record(
            slug=epoch.slug,
            condition_id=epoch.condition_id,
            question=epoch.question,
            epoch_start=_iso(epoch.start),
            epoch_end=_iso(epoch.end),
            up_token=epoch.up_token,
            down_token=epoch.down_token,
            created_at=_iso(epoch.created_at),
            created_ahead_s=created_ahead,
            tick_size=_num(epoch.tick_size),
            min_order_size=_num(epoch.min_order_size),
            seconds_delay=epoch.seconds_delay,
            strike_chainlink=self._strike.chainlink,
            strike_twap30=self._strike.twap30,
            strike_twap60=self._strike.twap60,
            strike_source=self._strike.source,
            strike_ts=_iso(self._strike.ts),
            discovered_at=_iso(datetime.now(UTC)),
        )
        if self._on_epoch_change is not None:
            self._on_epoch_change(epoch)

    def _close_current(self, epoch: Epoch) -> None:
        """La época venció: se fijan sus valores de cierre y se deja de seguirla."""
        settle = self._observation_at(epoch.end, SETTLE_MAX_GAP_S)
        self._record(
            slug=epoch.slug,
            settle_chainlink=None if settle is None else settle.chainlink,
            settle_twap30=None if settle is None else settle.twap30,
            settle_twap60=None if settle is None else settle.twap60,
            settle_ts=_iso(None if settle is None else settle.ts),
        )
        self._closed.add(epoch.slug)
        self._current = None
        self._strike = MISSED
        if self._on_epoch_change is not None:
            self._on_epoch_change(None)

    def _strike_at(self, start: datetime) -> Strike:
        """Strike observado en la apertura, o `MISSED` si no hay observación bastante cerca.

        Se usa `chainlink` y no `binance` porque el strike vive en unidades del oráculo:
        compararlo contra un spot de Binance sin corregir es exactamente el sesgo constante
        de §2.1.b, y aquí no hay corrección de basis que valga — el basis corrige el spot,
        no el strike.
        """
        observation = self._observation_at(start, STRIKE_MAX_GAP_S)
        if observation is None or observation.chainlink is None:
            return MISSED
        return Strike(
            value=observation.chainlink,
            source="open",
            ts=observation.ts,
            chainlink=observation.chainlink,
            twap30=observation.twap30,
            twap60=observation.twap60,
        )

    def _observation_at(self, target: datetime, max_gap_s: float) -> Observation | None:
        """La observación más cercana a `target`, si está dentro de la tolerancia."""
        best: Observation | None = None
        best_gap = float("inf")
        for observation in self._observations:
            gap = abs((observation.ts - target).total_seconds())
            if gap < best_gap:
                best, best_gap = observation, gap
        if best is None or best_gap > max_gap_s:
            return None
        return best

    def _record(self, **values: Any) -> None:
        """Escribe en `epochs` y añade una línea al journal inmutable.

        El journal es append-only y existe porque `state.db` es un estado mutable: si se
        corrompe o se borra, el journal sigue teniendo todos los desenlaces, que son el
        único insumo irrepetible de la Fase 0 — una época que ya resolvió no se vuelve a
        observar nunca.
        """
        values["updated_at"] = _iso(datetime.now(UTC))
        self._store.upsert_epoch(values)
        self._journal.parent.mkdir(parents=True, exist_ok=True)
        with self._journal.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(values, default=str, ensure_ascii=False) + "\n")


def _iso(ts: datetime | None) -> str | None:
    return None if ts is None else ts.astimezone(UTC).isoformat()


def _parse_ts(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    with contextlib.suppress(ValueError):
        return datetime.fromisoformat(value).astimezone(UTC)
    return None


def _num(value: Any) -> float | None:
    return None if value is None else float(value)
