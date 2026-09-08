"""Tests del grabador: esquema, durabilidad en disco, época y banderas de halt.

Lo que se vigila aquí no es que el código corra, es que **no se pierdan datos**. La Fase 0
son 14 días irrepetibles: una época que ya resolvió no se vuelve a observar, y un archivo
ilegible obliga a reiniciar el contador. Así que los tests que más pesan son los de muerte
brusca, cambio de hora y reinicio.

Ninguno toca la red. El bucle de 1 Hz es una función pura sobre `FeedState` (`tick`), y esa
es justamente la razón de que lo sea.
"""

from __future__ import annotations

import math
import os
import signal
import subprocess
import sys
import textwrap
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from polybtc.config import Settings
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
from polybtc.recorder import halts
from polybtc.recorder.engine import Recorder
from polybtc.recorder.epochs import EpochTracker
from polybtc.recorder.schema import ModelPoint, Sample, build_schema, to_record_batch
from polybtc.recorder.sink import ParquetSink, read_part, recover_orphans
from polybtc.recorder.status import StatusReader, StatusStore

WINDOWS = (60.0, 900.0)
T0 = datetime(2026, 9, 8, 17, 30, 0, tzinfo=UTC)
ORACLE_BASE = 78000.0


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        data_dir=tmp_path / "data",
        state_db_path=tmp_path / "data" / "state.db",
        twap_window_hypotheses_s=WINDOWS,
        basis_min_samples=3,
        recorder_flush_s=60.0,
    )


def sample(ts: datetime, **overrides: object) -> Sample:
    base: dict[str, object] = {
        "chainlink": 78570.0,
        "twap30": 78571.0,
        "binance_mid": 78584.0,
        "basis_usd": -14.3,
        "tau_s": 300.0,
        "models": {
            60.0: ModelPoint(0.52, 0.521, 0.50, 0.54, 78570.0, 1e-6, "window_pending", 0.002)
        },
    }
    base.update(overrides)
    return Sample(ts=ts, **base)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Esquema
# ---------------------------------------------------------------------------


def test_esquema_tiene_una_columna_por_hipotesis_de_ventana() -> None:
    """§8 sigue abierta: el Parquet lleva las DOS lecturas, no una elegida a ojo."""
    names = set(build_schema(WINDOWS).names)
    assert {"fair_w60", "fair_band_w60", "regime_w60"} <= names
    assert {"fair_w900", "fair_band_w900", "regime_w900"} <= names


def test_hipotesis_sin_evaluar_queda_nula_no_a_cero() -> None:
    """Un fair que no se pudo calcular debe distinguirse de un fair que salió 0."""
    batch = to_record_batch([sample(T0)], WINDOWS)
    assert batch.column("fair_w60").to_pylist() == [0.52]
    assert batch.column("fair_w900").to_pylist() == [None]


def test_columnas_ausentes_del_sample_llegan_nulas() -> None:
    batch = to_record_batch([Sample(ts=T0)], WINDOWS)
    assert batch.column("chainlink").to_pylist() == [None]
    assert batch.column("basis_ready").to_pylist() == [False]


# ---------------------------------------------------------------------------
# Durabilidad en disco: lo que decide si los 14 días valen algo
# ---------------------------------------------------------------------------


def test_cierre_ordenado_deja_parquet_legible(tmp_path: Path) -> None:
    with ParquetSink(tmp_path, WINDOWS) as sink:
        for i in range(5):
            sink.append(sample(T0 + timedelta(seconds=i)))
        sink.flush()

    written = list(tmp_path.rglob("*.parquet"))
    assert [p.name for p in written] == ["17.parquet"]
    assert pq.read_table(written[0]).num_rows == 5
    assert not list(tmp_path.rglob("*.part")), "el .part se borra al cerrar la hora"


def test_el_cambio_de_hora_reparte_las_filas_por_su_propia_hora(tmp_path: Path) -> None:
    """La partición es por hora de la MUESTRA, no del volcado: un volcado puede cruzarla."""
    sink = ParquetSink(tmp_path, WINDOWS)
    sink.append(sample(datetime(2026, 9, 8, 17, 59, 59, tzinfo=UTC)))
    sink.append(sample(datetime(2026, 9, 8, 18, 0, 0, tzinfo=UTC)))
    sink.append(sample(datetime(2026, 9, 8, 18, 0, 1, tzinfo=UTC)))
    sink.close()

    hour17 = pq.read_table(tmp_path / "2026/09/08/17.parquet")
    hour18 = pq.read_table(tmp_path / "2026/09/08/18.parquet")
    assert (hour17.num_rows, hour18.num_rows) == (1, 2)


def test_muerte_brusca_conserva_lo_ya_volcado(tmp_path: Path) -> None:
    """Sin cerrar: el `.part` sigue siendo legible y `recover_orphans` lo convierte.

    Es la propiedad por la que el almacén primario es Arrow IPC y no un `ParquetWriter`
    abierto: un Parquet sin footer no sería un archivo incompleto, sería uno ilegible que
    además rompería la lectura del directorio entero en el Sprint 5.
    """
    sink = ParquetSink(tmp_path, WINDOWS)
    for i in range(3):
        sink.append(sample(T0 + timedelta(seconds=i)))
    sink.flush()
    del sink  # el proceso muere aquí: nadie llama a close()

    part = next(tmp_path.rglob("*.part"))
    assert sum(b.num_rows for b in read_part(part)) == 3

    recovered = recover_orphans(tmp_path)
    assert [p.name for p in recovered] == ["17.parquet"]
    assert pq.read_table(recovered[0]).num_rows == 3
    assert not list(tmp_path.rglob("*.part"))


def test_part_truncado_conserva_los_bloques_completos(tmp_path: Path) -> None:
    """Un corte de luz parte el archivo por la mitad. Lo anterior tiene que sobrevivir."""
    sink = ParquetSink(tmp_path, WINDOWS)
    for i in range(4):
        sink.append(sample(T0 + timedelta(seconds=i)))
    sink.flush()
    sink.append(sample(T0 + timedelta(seconds=9)))
    sink.flush()
    part = next(tmp_path.rglob("*.part"))
    size = part.stat().st_size
    del sink

    with part.open("r+b") as handle:  # cortamos el último bloque a la mitad
        handle.truncate(size - 40)

    assert sum(b.num_rows for b in read_part(part)) == 4


def test_reinicio_en_la_misma_hora_no_pisa_lo_anterior(tmp_path: Path) -> None:
    """Dos corridas dentro de la misma hora dan dos archivos, no uno sobrescrito."""
    with ParquetSink(tmp_path, WINDOWS) as first:
        first.append(sample(T0))
    with ParquetSink(tmp_path, WINDOWS) as second:
        second.append(sample(T0 + timedelta(seconds=1)))

    names = sorted(p.name for p in tmp_path.rglob("*.parquet"))
    assert names == ["17.1.parquet", "17.parquet"]


def test_part_vacio_se_limpia_en_vez_de_acumularse(tmp_path: Path) -> None:
    directory = tmp_path / "2026/09/08"
    directory.mkdir(parents=True)
    (directory / "17.arrow.part").write_bytes(b"")

    assert recover_orphans(tmp_path) == []
    assert not list(tmp_path.rglob("*.part"))


def test_sigterm_cierra_la_hora_sin_corromper_el_archivo(tmp_path: Path) -> None:
    """El criterio literal de §6: un `SIGTERM` deja el Parquet completo y legible.

    Se hace en un proceso de verdad porque es la única forma de que la señal sea real:
    simular el manejador probaría el simulador, no el apagado.
    """
    script = textwrap.dedent(
        f"""
        import signal, sys, time
        from datetime import UTC, datetime, timedelta
        from pathlib import Path
        sys.path.insert(0, {str(Path(__file__).parent)!r})
        from test_recorder import sample, WINDOWS
        from polybtc.recorder.sink import ParquetSink

        sink = ParquetSink(Path({str(tmp_path)!r}), WINDOWS)
        signal.signal(signal.SIGTERM, lambda *_: (sink.close(), sys.exit(0)))
        t0 = datetime(2026, 9, 8, 17, 30, tzinfo=UTC)
        for i in range(10):
            sink.append(sample(t0 + timedelta(seconds=i)))
        Path({str(tmp_path / "ready")!r}).write_text("go")
        time.sleep(30)
        """
    )
    proc = subprocess.Popen([sys.executable, "-c", script])
    try:
        deadline = 20.0
        while not (tmp_path / "ready").exists() and deadline > 0:
            deadline -= 0.05
            _sleep(0.05)
        assert (tmp_path / "ready").exists(), "el proceso hijo no llegó a arrancar"
        proc.send_signal(signal.SIGTERM)
        assert proc.wait(timeout=20) == 0
    finally:
        if proc.poll() is None:
            proc.kill()

    written = list(tmp_path.rglob("*.parquet"))
    assert len(written) == 1
    assert pq.read_table(written[0]).num_rows == 10


def _sleep(seconds: float) -> None:
    import time

    time.sleep(seconds)


# ---------------------------------------------------------------------------
# state.db
# ---------------------------------------------------------------------------


def test_el_dashboard_no_puede_escribir_en_state_db(tmp_path: Path) -> None:
    """`mode=ro` es una garantía del motor, no una convención de estilo."""
    import sqlite3

    path = tmp_path / "state.db"
    with StatusStore(path) as store:
        store.write_status({"pid": 1, "halt_reasons": "", "halted": False})

    reader = StatusReader(path)
    status = reader.status()
    assert status is not None and status["pid"] == 1
    assert status["halted"] == 0, "los booleanos se guardan como 0/1, no como texto"

    conn = reader._connect()
    assert conn is not None
    with pytest.raises(sqlite3.OperationalError):
        conn.execute("UPDATE status SET pid = 99")


def test_status_nunca_se_ve_vacio_entre_escrituras(tmp_path: Path) -> None:
    """Se actualiza con UPSERT y no con DELETE+INSERT: el lector no ve un hueco."""
    path = tmp_path / "state.db"
    with StatusStore(path) as store:
        reader = StatusReader(path)
        for pid in range(1, 4):
            store.write_status({"pid": pid})
            status = reader.status()
            assert status is not None and status["pid"] == pid


def test_columna_inexistente_es_un_error_no_un_dato_perdido(tmp_path: Path) -> None:
    with StatusStore(tmp_path / "state.db") as store:
        with pytest.raises(KeyError):
            store.write_status({"no_existe": 1})


def test_la_epoca_se_completa_en_varias_pasadas(tmp_path: Path) -> None:
    """La fila nace al descubrirla y se actualiza cuando llega la resolución, horas después."""
    with StatusStore(tmp_path / "state.db") as store:
        store.upsert_epoch({"slug": "e1", "epoch_end": "2026-09-08T18:00:00+00:00"})
        assert [row["slug"] for row in store.pending_resolutions()] == ["e1"]

        store.upsert_epoch({"slug": "e1", "strike_chainlink": 78570.0})
        store.upsert_epoch({"slug": "e1", "resolved_outcome": "Up"})
        assert store.pending_resolutions() == []

        reader = StatusReader(tmp_path / "state.db")
        row = reader.epochs()[0]
        assert row["strike_chainlink"] == 78570.0, "un upsert parcial no borra lo anterior"
        assert row["resolved_outcome"] == "Up"


def test_el_dashboard_arranca_antes_que_el_grabador(tmp_path: Path) -> None:
    """Sin `state.db` todavía, el dashboard dice «no hay datos», no revienta."""
    reader = StatusReader(tmp_path / "no-existe.db")
    assert reader.status() is None
    assert reader.epochs() == []


# ---------------------------------------------------------------------------
# Época: strike, ventana observada y resolución
# ---------------------------------------------------------------------------


def epoch(start: datetime, slug: str | None = None) -> Epoch:
    """Época sintética con la frontera exacta, como la construye `discovery`."""
    return Epoch(
        slug=slug or f"btc-updown-15m-{int(start.timestamp())}",
        condition_id="0xabc",
        question="Bitcoin Up or Down",
        start=start,
        end=start + timedelta(seconds=900),
        up_token="tok-up",
        down_token="tok-down",
        tick_size=Decimal("0.01"),
        min_order_size=Decimal(5),
        seconds_delay=None,
        created_at=start - timedelta(days=1),
    )


def test_el_strike_se_toma_en_el_segundo_de_apertura(settings: Settings) -> None:
    """La época se adopta en el bucle de 1 Hz, no cuando la red se entera.

    Las épocas futuras ya existen con horas de antelación, así que la lista está llena
    antes de que empiece. Si la adopción dependiera del sondeo de red, el strike se tomaría
    hasta `epoch_discovery_poll_s` tarde y se saldría de la tolerancia — en cada época.
    """
    start = T0 + timedelta(seconds=5)
    with StatusStore(settings.state_db_path) as store:
        tracker = EpochTracker(settings, store)
        tracker._upcoming = [epoch(start)]

        for i in range(10):
            tracker.observe(T0 + timedelta(seconds=i), 78000.0 + i, None, None)

        assert tracker.current is not None
        assert tracker.strike.source == "open"
        assert tracker.strike.value == 78005.0, "el precio del segundo de apertura"


def test_sin_observacion_en_la_apertura_no_se_inventa_un_strike(settings: Settings) -> None:
    """Arrancar a mitad de época no produce un fair aproximado: produce ninguno."""
    start = T0
    with StatusStore(settings.state_db_path) as store:
        tracker = EpochTracker(settings, store)
        # La lista llega tarde: el grabador arrancó con la época ya empezada.
        tracker.observe(T0 + timedelta(seconds=400), 78000.0, None, None)
        tracker._upcoming = [epoch(start)]
        tracker.observe(T0 + timedelta(seconds=401), 78001.0, None, None)

        assert tracker.current is not None
        assert tracker.strike.source == "missed"
        assert not tracker.strike.usable


def test_la_media_observada_es_la_de_la_ventana_de_liquidacion(settings: Settings) -> None:
    """§3.3 pide la media sobre `[T−w, t]`, no la media móvil que publica RTDS."""
    with StatusStore(settings.state_db_path) as store:
        tracker = EpochTracker(settings, store)
        tracker._upcoming = [epoch(T0)]
        for i in range(900):
            tracker.observe(T0 + timedelta(seconds=i), 78000.0 + i, None, None)

        now = T0 + timedelta(seconds=880)
        # Ventana de 60 s: abre en t=840, y hasta t=880 hay 41 observaciones.
        assert tracker.observed_mean(60.0, now) == pytest.approx(78860.0)
        # Ventana de 900 s: es la época entera, media de 78000..78880.
        assert tracker.observed_mean(900.0, now) == pytest.approx(78440.0)
        # Y no es la media móvil que publicaría RTDS sobre `[t−60, t]`, que sería otra cosa.
        assert tracker.observed_mean(60.0, now) != pytest.approx(78850.0)


def test_la_epoca_se_cierra_sola_al_vencer_y_no_se_readopta(settings: Settings) -> None:
    """Vencida deja de seguirse, y no se vuelve a adoptar aunque siga en la lista."""
    start = T0 - timedelta(seconds=890)
    with StatusStore(settings.state_db_path) as store:
        tracker = EpochTracker(settings, store)
        tracker._upcoming = [epoch(start)]
        tracker.observe(start, 78000.0, None, None)
        adopted = tracker.current
        assert adopted is not None

        for i in range(12):
            tracker.observe(T0 + timedelta(seconds=i), 78100.0, 78101.0, 78102.0)

        assert tracker.current is None, "vencida: deja de haber mercado que seguir"
        row = StatusReader(settings.state_db_path).epochs()[0]
        assert row["settle_chainlink"] == 78100.0
        assert row["resolved_outcome"] is None, "el desenlace llega por red, no aquí"
        assert row["created_ahead_s"] == pytest.approx(86400.0)


def test_el_relevo_entre_epocas_no_deja_ningun_segundo_sin_mercado(settings: Settings) -> None:
    """No hay hueco entre épocas: la siguiente ya existe. Medido el 2026-09-08."""
    with StatusStore(settings.state_db_path) as store:
        tracker = EpochTracker(settings, store)
        tracker._upcoming = [epoch(T0), epoch(T0 + timedelta(seconds=900))]

        tracker.observe(T0, 78000.0, None, None)
        first = tracker.current
        assert first is not None

        tracker.observe(T0 + timedelta(seconds=900), 78050.0, None, None)
        second = tracker.current
        assert second is not None and second.slug != first.slug
        assert tracker.strike.value == 78050.0, "strike nuevo en el mismo segundo del relevo"


def test_cada_cambio_de_epoca_deja_linea_en_el_journal(settings: Settings) -> None:
    """El journal es append-only: si `state.db` se pierde, los desenlaces siguen ahí."""
    with StatusStore(settings.state_db_path) as store:
        tracker = EpochTracker(settings, store)
        tracker._upcoming = [epoch(T0)]
        tracker.observe(T0, 78000.0, None, None)

    journal = settings.data_dir / "epochs.jsonl"
    assert journal.exists()
    assert journal.read_text().strip().count("\n") == 0
    assert '"slug"' in journal.read_text()


# ---------------------------------------------------------------------------
# Banderas de halt
# ---------------------------------------------------------------------------


def test_se_reportan_todas_las_banderas_no_solo_la_primera() -> None:
    """Interesa saber si un instante estuvo ciego por una razón o por cuatro."""
    state = FeedState(staleness_s=3.0)
    flags = halts.evaluate(
        state,
        ((FEED_CHAINLINK, "chainlink"),),
        basis_ready=False,
        basis_clamped=True,
        sigma_ready=False,
        sigma_in_band=False,
        has_market=True,
        has_strike=False,
    )
    assert flags.halted
    assert flags.joined() == (
        "stale:chainlink,basis_sin_converger,basis_fuera_de_clamp,"
        "sigma_sin_muestras,sigma_fuera_de_banda,sin_strike"
    )


def test_sin_mercado_no_se_reporta_ademas_sin_strike() -> None:
    """El hueco de ~2 min entre épocas (§8.1) es un estado normal, no dos problemas."""
    state = FeedState(staleness_s=3.0)
    state.set_price(FEED_CHAINLINK, Decimal(78000))
    flags = halts.evaluate(
        state,
        ((FEED_CHAINLINK, "chainlink"),),
        basis_ready=True,
        basis_clamped=False,
        sigma_ready=True,
        sigma_in_band=True,
        has_market=False,
        has_strike=False,
    )
    assert flags.joined() == "sin_mercado"


def test_sin_banderas_se_podria_cotizar() -> None:
    state = FeedState(staleness_s=3.0)
    state.set_price(FEED_CHAINLINK, Decimal(78000))
    flags = halts.evaluate(
        state,
        ((FEED_CHAINLINK, "chainlink"),),
        basis_ready=True,
        basis_clamped=False,
        sigma_ready=True,
        sigma_in_band=True,
        has_market=True,
        has_strike=True,
    )
    assert not flags.halted
    assert flags.joined() == ""


# ---------------------------------------------------------------------------
# El bucle de 1 Hz, sin red
# ---------------------------------------------------------------------------


def feed(state: FeedState, chainlink: float, binance: float) -> None:
    state.set_price(FEED_CHAINLINK, Decimal(str(chainlink)))
    state.set_price(FEED_TWAP30, Decimal(str(chainlink)))
    state.set_price(FEED_TWAP60, Decimal(str(chainlink)))
    state.set_price(FEED_BINANCE_RELAY, Decimal(str(binance)))
    state.set_price(FEED_BINANCE_DIRECT, Decimal(str(binance)))


def test_un_tick_produce_una_fila_con_los_insumos_del_modelo(settings: Settings) -> None:
    """§9: sin σ, basis, `S_eff`, τ y la rama de varianza no se reconstruye una decisión."""
    recorder = Recorder(settings)
    state = recorder._state
    recorder._tracker._upcoming = [epoch(T0)]

    for i in range(120):
        now = T0 + timedelta(seconds=i)
        # Zigzag de ±0,015% por segundo: da σ ≈ 170% anual, dentro del piso y el techo de
        # §3.4. Una rampa suave daría σ ≈ 3,6% y el halt por vol fuera de banda saltaría —
        # que es lo correcto por parte del sistema, pero no es lo que este test mide.
        price = ORACLE_BASE * math.exp(0.00015 * (1 if i % 2 else -1))
        feed(state, price, price + 14.0)
        state.set_book("tok-up", Quote(Decimal("0.51"), Decimal("0.53")))
        state.set_book("tok-down", Quote(Decimal("0.47"), Decimal("0.49")))
        row = recorder.tick(now)

    assert row.epoch_slug == f"btc-updown-15m-{int(T0.timestamp())}"
    assert row.strike == pytest.approx(ORACLE_BASE * math.exp(-0.00015))
    assert row.strike_source == "open"
    assert row.tau_s == pytest.approx(900 - 119)
    assert row.basis_ready and row.basis_usd is not None
    assert row.spot_oracle is not None
    assert row.sigma_s is not None and row.sigma_s > 0
    assert row.up_bid == 0.51 and row.down_ask == 0.49

    # Las DOS hipótesis de §8 se evalúan; ninguna se elige aquí.
    assert set(row.models) == {60.0, 900.0}
    assert row.models[60.0].regime == "window_pending"
    assert row.models[900.0].regime == "window_open", "τ=781 s < w=900 s: la ventana ya corre"
    assert not row.halted

    recorder._sink.close()
    recorder._store.close()


def test_el_basis_sin_converger_deja_el_fair_nulo_y_la_bandera_puesta(settings: Settings) -> None:
    """§5.2: basis sin converger → no cotizar. En Fase 0 eso es una fila sin fair."""
    recorder = Recorder(settings)
    feed(recorder._state, 78000.0, 78014.0)
    row = recorder.tick(T0)

    assert row.models == {}
    assert "basis_sin_converger" in row.halt_reasons
    assert row.spot_oracle is None

    recorder._sink.close()
    recorder._store.close()


def test_el_cambio_de_epoca_olvida_los_libros_viejos(settings: Settings) -> None:
    """Los token ids rotan (§8.1): 14 días son ~2.700 libros que no pueden quedarse."""
    state = FeedState(staleness_s=3.0)
    state.set_book("viejo", Quote(Decimal("0.4"), Decimal("0.6")))
    assert state.age_s(book_feed("viejo")) is not None

    state.forget_book("viejo")
    assert state.book("viejo") is None
    assert state.age_s(book_feed("viejo")) is None
    assert state.is_stale(book_feed("viejo")), "sin dato es stale, no fresco"


def test_el_estado_vivo_llega_a_state_db_cada_tick(settings: Settings) -> None:
    recorder = Recorder(settings)
    feed(recorder._state, 78000.0, 78014.0)
    recorder.tick(T0)

    status = StatusReader(settings.state_db_path).status()
    assert status is not None
    assert status["pid"] == os.getpid()
    assert status["ticks"] == 1
    assert status["chainlink"] == 78000.0
    assert status["windows_json"] == '["w60","w900"]'

    recorder._sink.close()
    recorder._store.close()
