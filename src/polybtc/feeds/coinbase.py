"""Spot de Coinbase (BTC-USD) como alternativa a Binance. Público, sin credenciales.

Existe por una medición, no por gusto. Desde el VPS de Helsinki, el 2026-09-24:

| Endpoint | Resuelve a | RTT |
| --- | --- | --- |
| `stream.binance.com` y el resto de sus WS | AWS Tokio | **280 ms** |
| `ws-feed.exchange.coinbase.com` | borde de Cloudflare | **8 ms** |
| Polymarket (RTDS, CLOB, Gamma) | — | 3-5 ms |

**Todos los WebSocket de Binance salen de Tokio y no hay endpoint europeo.** Como el `fair`
se construye con el spot y el `mid` viene del libro de Polymarket, lo que contamina
`|fair − mid|` es la DIFERENCIA de frescura entre ambos: con Binance son ~275 ms, que a
τ=60 s son 3,3 céntimos de ruido — por encima del colchón de modelo de §3.5. No se arregla
cambiando de VPS: no hay ningún punto del planeta cerca de Tokio y de Londres a la vez.

Hay un segundo motivo, y puede pesar más que la latencia: **Coinbase cotiza BTC-USD, no
BTC-USDT**. §2.1.b describe el basis como «el bug silencioso más caro de este diseño» y lo
atribuye a dos causas: el desanclaje de USDT y la prima por plaza. Con Coinbase, la primera
desaparece — misma denominación que Chainlink. Queda solo la prima de plaza.

**Una diferencia estructural con Binance que hay que tener presente**: el canal `ticker` de
Coinbase emite **con cada operación**, no con cada cambio del top of book. El `bookTicker`
de Binance es lo segundo. En BTC-USD hay operaciones de sobra por segundo, pero en un
momento muy tranquilo el mid puede quedarse quieto entre trades, y eso lo vería el
estimador de σ como calma. El grabador registra la edad de este feed, así que si pasa, se
verá en los datos en vez de adivinarse.

Formato verificado en `docs.cdp.coinbase.com/exchange/websocket-feed/channels` el
2026-09-24: canal `ticker`, campos `best_bid` y `best_ask` como cadenas. Se ignora `price`
a propósito — es el último trade, y §3.4 pide el mid del libro porque el último trade
arrastra bid-ask bounce que inflaría σ de forma sistemática.
"""

from __future__ import annotations

import json

from websockets.asyncio.client import ClientConnection

from polybtc.config import Settings
from polybtc.feeds.parsing import as_decimal, loads_decimal
from polybtc.feeds.state import FEED_COINBASE, FeedState, Quote
from polybtc.feeds.ws import BackoffPolicy, run_feed_forever

FEED_NAME = "coinbase"

CHANNEL_TICKER = "ticker"
CHANNEL_HEARTBEAT = "heartbeat"
"""El heartbeat emite una vez por segundo para el producto suscrito.

No es decorativo: sin él, `recv_timeout_s` mediría el silencio entre operaciones y podría
declarar muerta una conexión perfectamente viva en un minuto tranquilo. Con el heartbeat,
el silencio real significa siempre que la conexión se cayó.
"""


def build_subscribe_message(settings: Settings) -> str:
    """Suscripción a `ticker` y `heartbeat`.

    La documentación avisa de que el mensaje es obligatorio y que el servidor desconecta si
    no lo recibe en 5 segundos, así que se manda en el propio `on_connect`.
    """
    return json.dumps(
        {
            "type": "subscribe",
            "product_ids": [settings.coinbase_product_id],
            "channels": [CHANNEL_TICKER, CHANNEL_HEARTBEAT],
        }
    )


def parse_ticker(raw: str, settings: Settings) -> Quote | None:
    """Mensaje `ticker` → `Quote`, o `None` si no lo es.

    Se filtra por `product_id` además de por tipo: una suscripción puede traer varios
    productos y mezclar BTC-USD con otra cosa sería un error sin síntoma, igual que el de
    §2.1 con los símbolos de RTDS.
    """
    message = loads_decimal(raw)
    if not isinstance(message, dict):
        return None
    if message.get("type") != CHANNEL_TICKER:
        return None
    if message.get("product_id") != settings.coinbase_product_id:
        return None

    bid = as_decimal(message.get("best_bid"))
    ask = as_decimal(message.get("best_ask"))
    if bid is None or ask is None or bid <= 0 or ask <= 0 or bid >= ask:
        return None
    return Quote(bid=bid, ask=ask)


def handle_message(raw: str, state: FeedState, settings: Settings) -> None:
    quote = parse_ticker(raw, settings)
    if quote is None:
        return
    state.set_price(FEED_COINBASE, quote.mid)


async def run(state: FeedState, settings: Settings) -> None:
    """Tarea de larga duración: mantiene el spot de Coinbase volcando a `state`.

    Sin `text_ping`: Coinbase usa el ping del **protocolo** WebSocket, que `websockets`
    contesta solo. El frame de texto `PING` que exigen RTDS y el CLOB aquí sería basura.
    """

    async def on_connect(ws: ClientConnection) -> None:
        await ws.send(build_subscribe_message(settings))

    await run_feed_forever(
        name=FEED_NAME,
        url=settings.coinbase_ws_url,
        on_connect=on_connect,
        on_message=lambda raw: handle_message(raw, state, settings),
        state=state,
        open_timeout_s=settings.ws_open_timeout_s,
        recv_timeout_s=settings.ws_recv_timeout_s,
        backoff=BackoffPolicy(settings.ws_backoff_base_s, settings.ws_backoff_max_s),
    )
