"""Tests del dashboard: solo lectura, tolerante a que no haya datos, y sin acoplarse al bot.

La propiedad que más importa no es que pinte bien, es que **no pueda hacer daño**. El
criterio de salida del sprint es que matar el dashboard con `kill -9` no le quite un solo
tick al grabador, y eso se sostiene sobre dos cosas comprobables aquí: procesos separados
—el dashboard no importa nada de ejecución— y WAL, que deja al escritor escribir mientras
alguien lee.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from polybtc.config import Settings
from polybtc.dashboard import history
from polybtc.dashboard.app import create_app
from polybtc.recorder.schema import ModelPoint, Sample
from polybtc.recorder.sink import ParquetSink
from polybtc.recorder.status import StatusReader, StatusStore

WINDOWS = (60.0, 900.0)
NOW = datetime(2026, 9, 8, 17, 45, 0, tzinfo=UTC)


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        data_dir=tmp_path / "data",
        state_db_path=tmp_path / "data" / "state.db",
        twap_window_hypotheses_s=WINDOWS,
    )


def write_hour(root: Path, start: datetime, rows: int = 120) -> None:
    """Una hora sintética con fair, mid y τ decreciente, como la grabaría el bot."""
    with ParquetSink(root, WINDOWS) as sink:
        for i in range(rows):
            tau = 900.0 - i * 7.0
            sink.append(
                Sample(
                    ts=start + timedelta(seconds=i),
                    chainlink=78000.0 + i,
                    binance_mid=78014.0 + i,
                    basis_usd=-14.0 - (i % 5) * 0.2,
                    sigma_annual=0.6,
                    tau_s=max(1.0, tau),
                    up_bid=0.50,
                    up_ask=0.52,
                    up_tick=0.01,
                    models={
                        60.0: ModelPoint(
                            fair=0.55 + (i % 7) * 0.01,
                            fair_band=0.55 + (i % 7) * 0.01,
                            band_low=0.54,
                            band_high=0.56,
                            spot_eff=78000.0,
                            variance=1e-6,
                            regime="window_pending",
                            delta_per_dollar=0.002,
                        )
                    },
                )
            )
        sink.flush()


# ---------------------------------------------------------------------------
# Tolerancia: el dashboard arranca antes que el grabador y le sobrevive
# ---------------------------------------------------------------------------


def test_sin_grabador_el_dashboard_responde_igual(settings: Settings) -> None:
    """Todavía no hay `state.db` ni Parquet. Eso es «no hay datos», no un error 500."""
    with TestClient(create_app(settings)) as client:
        payload = client.get("/api/status").json()
        assert payload["connected"] is False
        assert payload["status"] is None
        assert payload["windows"] == ["w60", "w900"]

        assert client.get("/api/series").json()["points"] == 0
        assert client.get("/api/edge").json()["windows"]["w60"]["samples"] == 0
        assert client.get("/api/epochs").json() == {"epochs": []}


def test_los_umbrales_viajan_con_el_dato(settings: Settings) -> None:
    """El semáforo usa el umbral del grabador. Uno propio en el HTML mentiría."""
    with TestClient(create_app(settings)) as client:
        thresholds = client.get("/api/status").json()["thresholds"]
    assert thresholds["staleness_s"] == settings.staleness_s
    assert thresholds["basis_min_samples"] == settings.basis_min_samples


def test_el_index_se_sirve_sin_cdn(settings: Settings) -> None:
    """Nada de CDN: el grabador vive en una máquina cuya red puede fallar (§6, Fase 0)."""
    with TestClient(create_app(settings)) as client:
        page = client.get("/").text
    assert "polybtc" in page
    assert "http://" not in page and "https://" not in page


def test_no_hay_panel_de_pnl(settings: Settings) -> None:
    """En Fase 0 no se opera. Un PnL a cero en pantalla invita a saltarse la fase (§6)."""
    with TestClient(create_app(settings)) as client:
        page = client.get("/").text.lower()
    assert "pnl" in page, "sí se menciona, pero solo para decir que no lo hay"
    assert "no se opera" in page


# ---------------------------------------------------------------------------
# Estado vivo
# ---------------------------------------------------------------------------


def test_el_estado_vivo_llega_al_api(settings: Settings) -> None:
    with StatusStore(settings.state_db_path) as store:
        store.write_status(
            {"pid": 42, "basis_usd": -14.3, "halt_reasons": "stale:twap30", "halted": True}
        )
    with TestClient(create_app(settings)) as client:
        payload = client.get("/api/status").json()
    assert payload["connected"] is True
    assert payload["status"]["basis_usd"] == -14.3
    assert payload["status"]["halt_reasons"] == "stale:twap30"


def test_el_grabador_sigue_escribiendo_con_el_dashboard_leyendo(settings: Settings) -> None:
    """La base del criterio de salida: WAL: leer no bloquea escribir, ni al revés.

    Es lo que hace que `kill -9` sobre el dashboard no pueda costarle un tick al grabador —
    ni siquiera si muere con una lectura a medias, porque nunca tuvo nada que soltar.
    """
    with StatusStore(settings.state_db_path) as store:
        store.write_status({"pid": 1})
        reader = StatusReader(settings.state_db_path)
        assert reader.status() is not None

        for pid in range(2, 20):
            store.write_status({"pid": pid})
        # El lector "muere" sin cerrar nada, y el escritor sigue como si nada.
        del reader
        store.write_status({"pid": 99})

    assert StatusReader(settings.state_db_path).status()["pid"] == 99  # type: ignore[index]


# ---------------------------------------------------------------------------
# Paneles: cada uno responde a una pregunta de §6
# ---------------------------------------------------------------------------


def test_la_hora_en_curso_se_lee_antes_de_cerrarse(settings: Settings) -> None:
    """Sin leer el `.part`, el panel iría hasta una hora por detrás de la realidad."""
    root = settings.data_dir
    sink = ParquetSink(root, WINDOWS)
    sink.append(Sample(ts=NOW, basis_usd=-14.0))
    sink.flush()  # queda un .part abierto, como en una hora en curso

    panel = history.series_panel(root, 6, [history.Window("w60")], NOW)
    assert panel["points"] == 1
    assert panel["basis_usd"] == [-14.0]
    sink.close()


def test_el_histograma_marca_las_dos_lineas_de_decision(settings: Settings) -> None:
    """1,75¢ es la fee taker (§2.2) y 3¢ el colchón de modelo (§3.5)."""
    write_hour(settings.data_dir, NOW - timedelta(minutes=30))
    panel = history.edge_panel(settings.data_dir, 6, [history.Window("w60")], NOW)

    assert panel["fee_taker"] == pytest.approx(0.0175)
    assert panel["cushion"] == pytest.approx(0.03)
    w60 = panel["windows"]["w60"]
    assert w60["samples"] == 120
    # fair ∈ [0.55, 0.61] contra un mid de 0.51: todo por encima de los dos umbrales.
    assert w60["above_fee"] == 1.0
    assert w60["above_cushion"] == 1.0
    assert sum(w60["histogram"]["counts"]) == 120


def test_el_desglose_por_tau_separa_latencia_de_modelo(settings: Settings) -> None:
    """Pregunta 6 de §6: si el edge solo vive bajo 30 s, es latencia y no es nuestro (§2.3)."""
    write_hour(settings.data_dir, NOW - timedelta(minutes=30))
    panel = history.edge_panel(settings.data_dir, 6, [history.Window("w60")], NOW)

    buckets = panel["windows"]["w60"]["by_tau"]
    assert [(b["low"], b["high"]) for b in buckets][:3] == [(0, 10), (10, 30), (30, 60)]
    assert sum(b["samples"] for b in buckets) > 0
    assert all(b["mean"] is None or b["mean"] >= 0 for b in buckets)


def test_ambas_hipotesis_de_ventana_se_presentan(settings: Settings) -> None:
    """§8 sigue abierta: enseñar una sola sería elegir la respuesta antes del dato."""
    write_hour(settings.data_dir, NOW - timedelta(minutes=30))
    windows = [history.Window("w60"), history.Window("w900")]
    panel = history.edge_panel(settings.data_dir, 6, windows, NOW)

    assert set(panel["windows"]) == {"w60", "w900"}
    assert panel["windows"]["w900"]["samples"] == 0, "no se evaluó, y se ve que no se evaluó"


def test_una_columna_que_no_existe_no_tumba_el_panel(settings: Settings) -> None:
    """Si cambia la configuración de hipótesis, los archivos viejos siguen leyéndose."""
    write_hour(settings.data_dir, NOW - timedelta(minutes=30))
    panel = history.edge_panel(settings.data_dir, 6, [history.Window("w1234")], NOW)
    assert panel["windows"]["w1234"]["samples"] == 0


# ---------------------------------------------------------------------------
# Spread: pregunta 1 de §6, y lo que impide leer mal la 6
# ---------------------------------------------------------------------------


def test_el_histograma_de_spread_usa_una_escala_fija_no_el_tick(settings: Settings) -> None:
    """Escalones de 0,1¢ y no «múltiplos del tick»: el tick cambia con el precio.

    Con barras en múltiplos del tick, un `1` significaría 1¢ en las filas de régimen grueso
    y 0,1¢ en las de régimen fino, mezcladas en el mismo histograma.
    """
    with ParquetSink(settings.data_dir, WINDOWS) as sink:
        for i, (bid, ask) in enumerate([(0.50, 0.51), (0.50, 0.51), (0.50, 0.52), (0.40, 0.60)]):
            sink.append(
                Sample(
                    ts=NOW + timedelta(seconds=i),
                    up_bid=bid,
                    up_ask=ask,
                    up_tick=0.01,
                    tau_s=100.0,
                )
            )
        sink.flush()

    panel = history.book_panel(settings.data_dir, 6, NOW + timedelta(minutes=1), tick=0.01)
    by_step = {row["steps"]: row["count"] for row in panel["by_tick"]}
    assert by_step[10] == 2, "1¢ son diez escalones de 0,1¢"
    assert by_step[20] == 2, "2¢ cae justo en el borde, y 20¢ desborda al mismo bucket"
    assert panel["resolution"] == pytest.approx(0.001)
    assert panel["ticks_seen"] == [0.01]


def test_pegado_al_tick_se_juzga_con_el_tick_de_cada_fila(settings: Settings) -> None:
    """Medio céntimo es un libro al suelo con tick 0,001 e imposible con tick 0,01.

    Es el hallazgo del 2026-09-11: el tick sigue al precio, no al mercado. Juzgar las dos
    filas con un tick fijo daría la respuesta contraria en una de las dos.
    """
    with ParquetSink(settings.data_dir, WINDOWS) as sink:
        # Régimen fino, deep OTM: 1 tick de spread. Está pegado al suelo.
        sink.append(Sample(ts=NOW, up_bid=0.003, up_ask=0.004, up_tick=0.001, tau_s=100.0))
        # Régimen grueso, cerca del medio: 5 ticks de spread. No lo está.
        sink.append(
            Sample(
                ts=NOW + timedelta(seconds=1),
                up_bid=0.50,
                up_ask=0.55,
                up_tick=0.01,
                tau_s=99.0,
            )
        )
        sink.flush()

    panel = history.book_panel(settings.data_dir, 6, NOW + timedelta(minutes=1), tick=0.01)
    assert panel["at_floor"] == pytest.approx(0.5), "uno sí y otro no"
    assert panel["ticks_seen"] == [0.001, 0.01], "los dos regímenes conviven en la ventana"


def test_una_fila_sin_tick_grabado_cae_al_respaldo(settings: Settings) -> None:
    """Los Parquet anteriores a la columna `up_tick` siguen siendo legibles."""
    with ParquetSink(settings.data_dir, WINDOWS) as sink:
        sink.append(Sample(ts=NOW, up_bid=0.50, up_ask=0.51, tau_s=100.0))
        sink.flush()

    panel = history.book_panel(settings.data_dir, 6, NOW + timedelta(minutes=1), tick=0.01)
    assert panel["at_floor"] == 1.0
    assert panel["ticks_seen"] == [0.01], "el respaldo de la configuración"


def test_el_desbordamiento_no_se_esconde(settings: Settings) -> None:
    """Un libro que se abre a 80¢ dice cuándo NO se puede cotizar. Ocultarlo adorna."""
    with ParquetSink(settings.data_dir, WINDOWS) as sink:
        sink.append(Sample(ts=NOW, up_bid=0.10, up_ask=0.90, up_tick=0.01, tau_s=50.0))
        sink.flush()

    panel = history.book_panel(settings.data_dir, 6, NOW + timedelta(minutes=1), tick=0.01)
    overflow = [row for row in panel["by_tick"] if row["overflow"]]
    assert len(overflow) == 1
    assert overflow[0]["count"] == 1
    assert panel["stats"]["max"] == pytest.approx(0.80)
    assert panel["at_floor"] == 0.0


def test_el_spread_por_tau_usa_los_mismos_cortes_que_el_edge(settings: Settings) -> None:
    """Las dos tablas se leen fila a fila: con cortes distintos no significarían nada."""
    write_hour(settings.data_dir, NOW - timedelta(minutes=30))
    book = history.book_panel(settings.data_dir, 6, NOW, tick=0.01)
    edge = history.edge_panel(settings.data_dir, 6, [history.Window("w60")], NOW)

    cortes_book = [(r["low"], r["high"]) for r in book["by_tau"]]
    cortes_edge = [(r["low"], r["high"]) for r in edge["windows"]["w60"]["by_tau"]]
    assert cortes_book == cortes_edge == list(history.TAU_BUCKETS)


def test_la_complementariedad_delata_un_libro_roto(settings: Settings) -> None:
    """Los dos outcomes suman 1. Cuando no, casi siempre es el feed, no una oportunidad."""
    with ParquetSink(settings.data_dir, WINDOWS) as sink:
        sink.append(
            Sample(ts=NOW, up_bid=0.50, up_ask=0.52, down_bid=0.48, down_ask=0.50, tau_s=100.0)
        )
        sink.append(
            Sample(
                ts=NOW + timedelta(seconds=1),
                up_bid=0.50,
                up_ask=0.52,
                down_bid=0.70,
                down_ask=0.72,
                tau_s=99.0,
            )
        )
        sink.flush()

    panel = history.book_panel(settings.data_dir, 6, NOW + timedelta(minutes=1), tick=0.01)
    assert panel["complementarity"]["n"] == 2
    # up_mid=0.51 con down_mid=0.71 suman 1.22: el libro no cuadra por 22¢.
    assert panel["complementarity"]["max"] == pytest.approx(0.22), "el libro descuadrado"
    assert panel["complementarity"]["min"] == pytest.approx(0.0)


def test_el_panel_de_spread_llega_al_api_y_tolera_no_tener_datos(settings: Settings) -> None:
    """Los endpoints leen del reloj real, así que el fixture se escribe en la hora de verdad."""
    with TestClient(create_app(settings)) as client:
        vacio = client.get("/api/book").json()
        assert vacio["samples"] == 0
        assert vacio["tick"] == float(settings.default_tick_size)

    write_hour(settings.data_dir, datetime.now(UTC) - timedelta(minutes=2), rows=30)
    with TestClient(create_app(settings)) as client:
        panel = client.get("/api/book?hours=6").json()
    assert panel["samples"] == 30
    assert panel["stats"]["mean"] == pytest.approx(0.02), "el fixture escribe 0.50/0.52"
    assert panel["at_floor"] == 0.0, "2¢ no es el suelo de 1¢"


def test_solo_se_leen_las_horas_pedidas(settings: Settings) -> None:
    """Con 14 días grabados, un panel de 6 h no puede abrir 336 archivos."""
    write_hour(settings.data_dir, NOW - timedelta(hours=20), rows=5)
    write_hour(settings.data_dir, NOW - timedelta(hours=1), rows=5)

    assert len(history.recent_files(settings.data_dir, 6, NOW)) == 1
    assert len(history.recent_files(settings.data_dir, 24, NOW)) == 2
