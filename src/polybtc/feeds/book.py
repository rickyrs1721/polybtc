"""Book por MarketSpec: best_bid_ask, price_change y tick_size_change (el tick cambia vivo).

Canal `market` del CLOB, público y sin credenciales. Verificado en
docs.polymarket.com/asyncapi.json (consultado 2026-09-02):

- URL `wss://ws-subscriptions-clob.polymarket.com/ws/market`.
- Suscripción inicial: `{"assets_ids": [...], "type": "market", "initial_dump": true,
  "custom_feature_enabled": true}`. `best_bid_ask` **solo llega** con
  `custom_feature_enabled` activo; sin él hay que derivar el top of book del snapshot.
- Eventos: `book`, `price_change`, `best_bid_ask`, `tick_size_change`, `last_trade_price`.
- Keepalive por frame de texto `PING`/`PONG`, igual que RTDS.

`last_trade_price` se ignora a propósito (§3.4): el último trade lleva bid-ask bounce.

Este módulo responde las preguntas 1 y 4 de la Fase 0 —distribución del spread y cuánto
sobrevive una orden en el book—, que son íntegramente de book y no del oráculo.
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

from websockets.asyncio.client import ClientConnection

from polybtc.config import Settings
from polybtc.feeds.parsing import as_decimal, loads_decimal
from polybtc.feeds.state import FeedState, Quote
from polybtc.feeds.ws import BackoffPolicy, run_feed_forever

FEED_NAME = "clob_market"

EVENT_BOOK = "book"
EVENT_PRICE_CHANGE = "price_change"
EVENT_BEST_BID_ASK = "best_bid_ask"
EVENT_TICK_SIZE_CHANGE = "tick_size_change"

TEXT_PING = "PING"


def build_subscribe_message(settings: Settings) -> str:
    """Suscripción inicial al canal `market` para los token ids configurados."""
    if not settings.token_ids:
        raise ValueError(
            "no hay token_ids configurados: el mercado objetivo es una decisión abierta de "
            "§8 y no se asume. Define POLYBTC_TOKEN_IDS antes de arrancar el feed de book."
        )
    return json.dumps(
        {
            "assets_ids": list(settings.token_ids),
            "type": "market",
            "initial_dump": True,
            "custom_feature_enabled": True,
        }
    )


def handle_message(raw: str, state: FeedState) -> None:
    """Enruta uno o varios eventos del canal `market` a `state`.

    El canal puede mandar un objeto suelto o un array de eventos en el mismo frame, así
    que se normalizan los dos casos.
    """
    message = loads_decimal(raw)
    if message is None:
        return
    events = message if isinstance(message, list) else [message]
    for event in events:
        if isinstance(event, dict):
            _handle_event(event, state)


async def subscribe(ws: ClientConnection, settings: Settings) -> None:
    await ws.send(build_subscribe_message(settings))


async def run(state: FeedState, settings: Settings) -> None:
    """Tarea de larga duración: mantiene el book vivo y volcando a `state`."""

    async def on_connect(ws: ClientConnection) -> None:
        await subscribe(ws, settings)

    await run_feed_forever(
        name=FEED_NAME,
        url=settings.clob_market_ws_url,
        on_connect=on_connect,
        on_message=lambda raw: handle_message(raw, state),
        state=state,
        text_ping=TEXT_PING,
        text_ping_interval_s=settings.rtds_text_ping_interval_s,
        open_timeout_s=settings.ws_open_timeout_s,
        recv_timeout_s=settings.ws_recv_timeout_s,
        backoff=BackoffPolicy(settings.ws_backoff_base_s, settings.ws_backoff_max_s),
    )


def _handle_event(event: dict[str, Any], state: FeedState) -> None:
    event_type = event.get("event_type")

    if event_type == EVENT_BOOK:
        quote = top_of_book(event)
        if quote is not None:
            state.set_book(quote)

    elif event_type == EVENT_BEST_BID_ASK:
        quote = _quote_from(event.get("best_bid"), event.get("best_ask"))
        if quote is not None:
            state.set_book(quote)

    elif event_type == EVENT_PRICE_CHANGE:
        _handle_price_change(event, state)

    elif event_type == EVENT_TICK_SIZE_CHANGE:
        tick = as_decimal(event.get("new_tick_size"))
        if tick is not None and tick > 0:
            # §4: se REEMPLAZA el valor guardado. El CLOB rechaza órdenes cuantizadas con
            # un tick obsoleto, y el rechazo no es descriptivo.
            state.set_tick_size(tick)


def _handle_price_change(event: dict[str, Any], state: FeedState) -> None:
    """`price_change` trae `best_bid`/`best_ask` ya calculados en cada entrada.

    Se toma la última entrada válida del array: son deltas en orden, y la posterior manda.
    """
    changes = event.get("price_changes")
    if not isinstance(changes, list):
        return
    for change in reversed(changes):
        if not isinstance(change, dict):
            continue
        quote = _quote_from(change.get("best_bid"), change.get("best_ask"))
        if quote is not None:
            state.set_book(quote)
            return


def top_of_book(snapshot: dict[str, Any]) -> Quote | None:
    """Mejor bid y mejor ask de un snapshot `book`.

    Se toma `max` de los bids y `min` de los asks en vez de un extremo del array porque la
    documentación no especifica el orden de `bids`/`asks`. Asumir un orden que la doc no
    promete es exactamente el tipo de detalle que se rompe en silencio.
    """
    bids = _prices(snapshot.get("bids"))
    asks = _prices(snapshot.get("asks"))
    if not bids or not asks:
        return None
    return _quote_from(max(bids), min(asks))


def _prices(levels: object) -> list[Decimal]:
    if not isinstance(levels, list):
        return []
    out: list[Decimal] = []
    for level in levels:
        if not isinstance(level, dict):
            continue
        price = as_decimal(level.get("price"))
        size = as_decimal(level.get("size"))
        if price is not None and price > 0 and (size is None or size > 0):
            out.append(price)
    return out


def _quote_from(bid: object, ask: object) -> Quote | None:
    """`Quote` solo si el par es coherente. Un cruce se descarta, no se propaga."""
    best_bid = as_decimal(bid)
    best_ask = as_decimal(ask)
    if best_bid is None or best_ask is None:
        return None
    if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
        return None
    return Quote(bid=best_bid, ask=best_ask)
