"""Punto de entrada del grabador: `uv run python -m polybtc.recorder`.

Deliberadamente NO se cuelga de `polybtc.main:main`, el comando `polybtc`. Ese comando
arranca el bot y a partir del Sprint 7 mueve dinero, por lo que está en la lista de
permisos que siempre pregunta. El grabador de la Fase 0 no toca dinero ni credenciales y
tiene que poder correr bajo `systemd` con `Restart=always` durante 14 días: mezclarlos
haría que el comando peligroso se volviera rutinario, que es como se pierde una salvaguarda.
"""

from __future__ import annotations

import asyncio
import contextlib

from polybtc.config import Settings
from polybtc.recorder.engine import Recorder


def main() -> None:
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(Recorder(Settings()).run())


if __name__ == "__main__":
    main()
