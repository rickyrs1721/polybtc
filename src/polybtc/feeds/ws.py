"""Bucle de conexión compartido: reconexión, backoff exponencial con jitter y PING de texto.

Los tres feeds necesitan exactamente lo mismo —conectar, suscribirse, leer para siempre y
volver a intentarlo cuando se caiga—, así que vive una vez aquí. La Fase 0 son 14 días de
grabación continua (§6): un feed que no se recupere solo de un corte de red no sirve, y un
hueco en los datos envenena el análisis.

El backoff es *full jitter* (`uniform(0, min(cap, base·2^n))`) y no exponencial puro a
propósito: si RTDS, el CLOB y Binance se cayeran a la vez, un backoff determinista los
haría reconectar en el mismo instante una y otra vez.

Hay dos "pings" distintos y confundirlos deja la conexión muerta:

- El ping del **protocolo** WebSocket (frame de control), que `websockets` gestiona solo.
- El frame de **texto** literal `PING` que exigen RTDS y el canal `market` del CLOB.
  Ese lo mandamos nosotros (`text_ping`). Verificado en
  docs.polymarket.com/market-data/realtime-data.md y docs.polymarket.com/asyncapi.json
  (consultados 2026-09-02).
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from websockets.asyncio.client import ClientConnection, connect

from polybtc.feeds.state import FeedState

OnConnect = Callable[[ClientConnection], Awaitable[None]]
OnMessage = Callable[[str], None]


@dataclass(frozen=True, slots=True)
class BackoffPolicy:
    """Full jitter. `base` y `max_delay` en segundos."""

    base_s: float = 0.5
    max_delay_s: float = 30.0

    def delay_for(self, attempt: int, *, rand: Callable[[], float] = random.random) -> float:
        """Espera para el intento `attempt` (0-based). Siempre en [0, max_delay_s]."""
        if attempt < 0:
            raise ValueError(f"attempt debe ser ≥ 0: {attempt}")
        ceiling = min(self.max_delay_s, self.base_s * (2.0**attempt))
        return rand() * ceiling


async def run_feed_forever(
    *,
    name: str,
    url: str,
    on_connect: OnConnect,
    on_message: OnMessage,
    state: FeedState,
    text_ping: str | None = None,
    text_ping_interval_s: float = 5.0,
    open_timeout_s: float = 10.0,
    recv_timeout_s: float = 30.0,
    backoff: BackoffPolicy | None = None,
) -> None:
    """Mantiene viva una suscripción hasta que se cancele la tarea.

    No traga `asyncio.CancelledError`: un apagado ordenado debe poder parar el feed. Todo
    lo demás —error de red, socket a medio cerrar, JSON inesperado del servidor— se trata
    como transitorio, porque a las 3 de la mañana del día 9 de la Fase 0 no hay nadie para
    reiniciarlo a mano.
    """
    policy = backoff or BackoffPolicy()
    attempt = 0

    while True:
        try:
            async with connect(url, open_timeout=open_timeout_s) as ws:
                await on_connect(ws)
                attempt = 0  # la conexión funcionó: el backoff vuelve a empezar de cero
                await _pump(
                    ws,
                    on_message=on_message,
                    text_ping=text_ping,
                    text_ping_interval_s=text_ping_interval_s,
                    recv_timeout_s=recv_timeout_s,
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            # Deliberadamente ancho: red, socket a medio cerrar, JSON inesperado del
            # servidor. Un feed jamás debe tumbar el proceso ni interrumpir la grabación.
            pass

        state.note_reconnect(name)
        await asyncio.sleep(policy.delay_for(attempt))
        attempt += 1


async def _pump(
    ws: ClientConnection,
    *,
    on_message: OnMessage,
    text_ping: str | None,
    text_ping_interval_s: float,
    recv_timeout_s: float,
) -> None:
    """Lee mensajes hasta que el socket muera o se quede mudo más de `recv_timeout_s`."""
    pinger = (
        asyncio.create_task(_text_ping_loop(ws, text_ping, text_ping_interval_s))
        if text_ping is not None
        else None
    )
    try:
        while True:
            raw = await asyncio.wait_for(ws.recv(), timeout=recv_timeout_s)
            on_message(raw if isinstance(raw, str) else raw.decode("utf-8"))
    finally:
        if pinger is not None:
            pinger.cancel()
            try:
                await pinger
            except asyncio.CancelledError:
                pass
            except Exception:
                pass


async def _text_ping_loop(ws: ClientConnection, text_ping: str, interval_s: float) -> None:
    while True:
        await asyncio.sleep(interval_s)
        await ws.send(text_ping)
