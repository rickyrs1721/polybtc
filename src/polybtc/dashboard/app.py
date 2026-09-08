"""FastAPI + Server-Sent Events. El estado vivo va por stream; lo pesado, cacheado.

Dos cadencias distintas y por un motivo:

- El **heartbeat** sale por SSE cada segundo. Es una lectura de una fila de SQLite: barata,
  y es lo que hace que el semáforo de feeds sirva para algo.
- Los **paneles históricos** se recalculan cada 30-60 s y se cachean. Leer 24 h de Parquet
  una vez por segundo no daría más información y sí competiría por disco con el grabador,
  que es el proceso que no puede permitirse ir lento.

Ningún endpoint escribe nada. La conexión a `state.db` es `mode=ro` (ver `recorder/status`).
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.responses import FileResponse, StreamingResponse

from polybtc.config import Settings
from polybtc.dashboard.history import Window, edge_panel, series_panel
from polybtc.recorder.status import StatusReader

SERIES_CACHE_S = 30.0
EDGE_CACHE_S = 60.0
STREAM_INTERVAL_S = 1.0
INDEX_PATH = Path(__file__).parent / "static" / "index.html"


class Cache:
    """Memo con caducidad. Un dashboard local no necesita nada más elaborado."""

    def __init__(self, ttl_s: float) -> None:
        self._ttl_s = ttl_s
        self._value: Any = None
        self._at: float = 0.0

    def get(self) -> Any | None:
        if self._value is None or time.monotonic() - self._at > self._ttl_s:
            return None
        return self._value

    def set(self, value: Any) -> Any:
        self._value = value
        self._at = time.monotonic()
        return value


def create_app(settings: Settings | None = None) -> FastAPI:
    """Construye la aplicación. Recibe `Settings` para poder testearla contra datos falsos."""
    config = settings or Settings()
    reader = StatusReader(config.state_db_path)
    windows = [Window(suffix=name) for name in _window_suffixes(config)]
    series_cache = Cache(SERIES_CACHE_S)
    edge_cache = Cache(EDGE_CACHE_S)

    app = FastAPI(title="polybtc · Fase 0", docs_url=None, redoc_url=None)

    @app.get("/")
    async def index() -> FileResponse:
        return FileResponse(INDEX_PATH)

    @app.get("/api/status")
    async def status() -> dict[str, Any]:
        return _envelope(reader, config)

    @app.get("/api/stream")
    async def stream() -> StreamingResponse:
        async def events() -> AsyncIterator[str]:
            # Sin `while True: yield` no habría dashboard: es el heartbeat. Si el cliente
            # se va, `StreamingResponse` cancela este generador y no queda nada colgando.
            while True:
                yield f"data: {json.dumps(_envelope(reader, config), default=str)}\n\n"
                await asyncio.sleep(STREAM_INTERVAL_S)

        return StreamingResponse(
            events(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @app.get("/api/series")
    async def series(hours: int | None = None) -> dict[str, Any]:
        if hours is None and (cached := series_cache.get()) is not None:
            return dict(cached)
        panel = await asyncio.to_thread(
            series_panel,
            config.data_dir,
            hours or config.dashboard_history_hours,
            windows,
            _now(),
        )
        return dict(series_cache.set(panel) if hours is None else panel)

    @app.get("/api/edge")
    async def edge(hours: int | None = None) -> dict[str, Any]:
        if hours is None and (cached := edge_cache.get()) is not None:
            return dict(cached)
        panel = await asyncio.to_thread(
            edge_panel,
            config.data_dir,
            hours or config.dashboard_histogram_hours,
            windows,
            _now(),
        )
        return dict(edge_cache.set(panel) if hours is None else panel)

    @app.get("/api/epochs")
    async def epochs(limit: int = 20) -> dict[str, Any]:
        return {"epochs": reader.epochs(limit)}

    return app


def _envelope(reader: StatusReader, config: Settings) -> dict[str, Any]:
    """Estado vivo más lo que el navegador necesita para pintar los umbrales.

    Los umbrales viajan con el dato en vez de estar duplicados en el HTML: son parámetros
    de `config.py` (§9) y un semáforo que use un umbral distinto del que usa el grabador
    miente, que es peor que no tener semáforo.
    """
    status = reader.status()
    return {
        "status": status,
        "connected": status is not None,
        "thresholds": {
            "staleness_s": config.staleness_s,
            "sigma_floor_annual": config.vol_floor_annual,
            "sigma_ceiling_annual": config.vol_ceiling_annual,
            "basis_clamp": config.basis_clamp,
            "basis_min_samples": config.basis_min_samples,
        },
        "windows": _window_suffixes(config),
    }


def _window_suffixes(config: Settings) -> list[str]:
    return [f"w{window:g}" for window in config.twap_window_hypotheses_s]


def _now() -> datetime:
    return datetime.now(UTC)


app = create_app()
"""Instancia por defecto, para `uvicorn polybtc.dashboard.app:app`."""
