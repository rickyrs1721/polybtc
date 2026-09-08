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


def test_solo_se_leen_las_horas_pedidas(settings: Settings) -> None:
    """Con 14 días grabados, un panel de 6 h no puede abrir 336 archivos."""
    write_hour(settings.data_dir, NOW - timedelta(hours=20), rows=5)
    write_hour(settings.data_dir, NOW - timedelta(hours=1), rows=5)

    assert len(history.recent_files(settings.data_dir, 6, NOW)) == 1
    assert len(history.recent_files(settings.data_dir, 24, NOW)) == 2
