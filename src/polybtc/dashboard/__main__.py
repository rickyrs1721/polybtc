"""`uv run python -m polybtc.dashboard` → uvicorn en localhost.

Se ata a `127.0.0.1` por defecto y no a `0.0.0.0`: este panel enseña el estado interno del
sistema y no tiene autenticación de ninguna clase. En el Sprint 4 el grabador vive en una
máquina encendida 24/7, y exponer esto a la red local sería regalar la ventana sin haberla
pedido. Para verlo desde otro equipo, un túnel SSH.
"""

from __future__ import annotations

import uvicorn

from polybtc.config import Settings


def main() -> None:
    settings = Settings()
    uvicorn.run(
        "polybtc.dashboard.app:app",
        host=settings.dashboard_host,
        port=settings.dashboard_port,
        log_level="warning",
    )


if __name__ == "__main__":
    main()
