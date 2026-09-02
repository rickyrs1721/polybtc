"""WS de spot como indicador adelantado; expone el mid del book, no el último trade (§3.4).

El TWAP es un promedio rezagado del spot, así que conocer el spot permite proyectar hacia
dónde va el TWAP. Es una ENTRADA del modelo, no el ancla (§2.1) — y llega en BTC**USDT**,
que no es la unidad del oráculo: sin la corrección de basis de §2.1.b este número no se
compara con un strike.

Se usa `bookTicker` y no `trade` a propósito (§3.4): el último trade mete bid-ask bounce,
que el estimador de σ interpretaría como volatilidad real e inflaría σ de forma
sistemática. El mid del top of book no tiene ese sesgo.

Stream verificado en developers.binance.com/docs/binance-spot-api-docs/web-socket-streams
(consultado 2026-09-02): `{base}/{symbol}@bookTicker`, con campos `b`/`B` (mejor bid y su
cantidad) y `a`/`A` (mejor ask y su cantidad).
"""

from __future__ import annotations

from websockets.asyncio.client import ClientConnection

from polybtc.config import Settings
from polybtc.feeds.parsing import as_decimal, loads_decimal
from polybtc.feeds.state import FEED_BINANCE_DIRECT, FeedState, Quote
from polybtc.feeds.ws import BackoffPolicy, run_feed_forever

FEED_NAME = "binance"


def stream_url(settings: Settings) -> str:
    """URL del raw stream. El símbolo va en minúsculas en el path."""
    return f"{settings.binance_ws_base_url}/{settings.binance_symbol.lower()}@bookTicker"


def parse_book_ticker(raw: str) -> Quote | None:
    """`bookTicker` → `Quote`, o `None` si el mensaje no lo es.

    Binance ha publicado los precios tanto como cadena (`"25.3519"`) como número JSON, y
    ambos se aceptan: con `parse_float=Decimal` el número tampoco pasa por `float`, así
    que en los dos casos el `Decimal` conserva los dígitos exactos del mensaje (§4).

    Un cruce (`bid >= ask`) se descarta en vez de propagarse: es un mensaje corrupto o
    fuera de orden, y un mid calculado sobre él envenenaría σ y el basis a la vez.
    """
    message = loads_decimal(raw)
    if not isinstance(message, dict):
        return None

    bid = as_decimal(message.get("b"))
    ask = as_decimal(message.get("a"))
    if bid is None or ask is None or bid <= 0 or ask <= 0 or bid >= ask:
        return None
    return Quote(bid=bid, ask=ask)


def handle_message(raw: str, state: FeedState) -> None:
    quote = parse_book_ticker(raw)
    if quote is None:
        return
    state.set_price(FEED_BINANCE_DIRECT, quote.mid)


async def run(state: FeedState, settings: Settings) -> None:
    """Tarea de larga duración: mantiene el spot directo vivo y volcando a `state`.

    Sin `text_ping`: Binance usa el ping del **protocolo** WebSocket, que `websockets`
    contesta solo. Mandarle el `PING` de texto que exige RTDS aquí sería un mensaje basura.
    """

    async def on_connect(_ws: ClientConnection) -> None:
        return None  # el raw stream ya lleva la suscripción en la URL

    await run_feed_forever(
        name=FEED_NAME,
        url=stream_url(settings),
        on_connect=on_connect,
        on_message=lambda raw: handle_message(raw, state),
        state=state,
        open_timeout_s=settings.ws_open_timeout_s,
        recv_timeout_s=settings.ws_recv_timeout_s,
        backoff=BackoffPolicy(settings.ws_backoff_base_s, settings.ws_backoff_max_s),
    )
