"""RTDS: TWAP Chainlink 30s/60s, el ancla de liquidación. Sin replay; PING cada 5 s (§2.1).

Cuatro suscripciones sobre **una sola conexión**, que es justo lo que permite el
cross-check gratis de §2.1.b: si el Binance directo y el Binance relayado por RTDS
divergen, el problema es tu red, no una oportunidad.

- `crypto_prices_chainlink` — spot del oráculo, referencia del basis.
- `crypto_prices_twap_thirty` / `_sixty` — el precio contra el que se liquida.
- `crypto_prices` — relay de Binance, para el cross-check.

Formato contrastado contra docs.polymarket.com/market-data/realtime-data.md y
docs.polymarket.com/market-data/chainlink-twap.md (consultados 2026-09-02) **y contra el
servidor real ese mismo día**. La doc describe una suscripción que el servidor acepta y
que luego no emite nada; tres detalles del protocolo vivo, los tres con fallo silencioso:

1. **NO se envía `filters`.** Es el hallazgo central y es contraintuitivo: el campo que la
   doc presenta como el modo de elegir símbolo es justo el que apaga el stream. Medido el
   mismo día, sobre el mismo tópico:

   | Forma de `filters`             | Acuse | Updates en vivo        |
   | ------------------------------ | ----- | ---------------------- |
   | `"btcusdt,ethusdt"` (la doc)   | no    | ninguno                |
   | `"{\\"symbol\\":\\"btcusdt\\"}"`  | sí    | ~ninguno, intermitente |
   | campo ausente                  | sí    | ~1/s, sostenido        |

   Sin `filters` llegan TODOS los símbolos (btc, eth, sol, doge…) y se descartan en
   cliente por `payload.symbol`. Es lo que hace el SDK oficial `polymarket-client`, cuyo
   tráfico se inspeccionó para llegar hasta aquí: manda estos mismos tópicos con guion
   bajo y sin `filters`. Cuesta un poco de ancho de banda —unos 40 mensajes/s en vez de
   4— y a cambio el feed existe.

2. **Un mismo tópico de respuesta puede traer dos feeds distintos.** Con `filters` puesto,
   `crypto_prices_chainlink` responde bajo `topic: "crypto_prices"`, el mismo que el relay
   de Binance. Enrutar por tópico a secas mete el precio del oráculo en el feed de Binance
   — y entonces el basis de §2.1.b sale idénticamente 1 y el sesgo que ese estimador
   existe para corregir vuelve entero, sin ningún síntoma. Se enruta por tópico **y**
   `payload.symbol`, que además es el filtro que ahora hay que hacer en cliente.

3. **Hay dos formas de payload.** Al suscribirse llega un `type: "subscribe"` con
   `payload.data`, un array de puntos a 1 s al día hasta el instante de la conexión;
   después llegan `type: "update"` con el payload plano que documenta la página. La doc
   solo describe el segundo. Se parsean los dos, y del backfill se toma el punto más
   reciente: sirve para arrancar con un valor bueno en vez de esperar al primer update.

Sin credenciales: RTDS es público. No hay snapshot ni replay tras una desconexión (§2.1),
así que lo único que protege la Fase 0 de un hueco es reconectar rápido y que la edad del
feed quede registrada mientras tanto.
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

from websockets.asyncio.client import ClientConnection

from polybtc.config import Settings
from polybtc.feeds.parsing import as_decimal, loads_decimal
from polybtc.feeds.state import (
    FEED_BINANCE_RELAY,
    FEED_CHAINLINK,
    FEED_TWAP30,
    FEED_TWAP60,
    FeedState,
)
from polybtc.feeds.ws import BackoffPolicy, run_feed_forever

FEED_NAME = "rtds"

TOPIC_CHAINLINK = "crypto_prices_chainlink"
TOPIC_TWAP_30 = "crypto_prices_twap_thirty"
TOPIC_TWAP_60 = "crypto_prices_twap_sixty"
TOPIC_BINANCE = "crypto_prices"

TEXT_PING = "PING"
"""Frame de TEXTO, no el ping del protocolo WS. Cada 5 s o la conexión se cae (§2.1)."""

_TWAP_TOPIC_TO_FEED = {TOPIC_TWAP_30: FEED_TWAP30, TOPIC_TWAP_60: FEED_TWAP60}
"""Los TWAP sí vuelven con su propio tópico, así que estos se enrutan directamente."""

_WINDOW_TOPIC = {30: TOPIC_TWAP_30, 60: TOPIC_TWAP_60}

_FULL_ACCURACY_SCALES = (Decimal(1), Decimal(10) ** 18)
"""Escalas plausibles de `full_accuracy_value`, y por qué se prueban las dos.

La documentación muestra el campo sin escalar en un tópico (`equity_prices`: `"189.4217"`)
y escalado a 18 decimales en otro (TWAP: `"65000500000000000000000"` para `value` 65000.5).
No está documentado cuál aplica a cada tópico, así que no se asume ninguna: se elige la que
reconstruye `value`, y si ninguna lo hace se usa `value`. Adivinar la escala aquí sería un
error de 18 órdenes de magnitud en el ancla de liquidación.
"""

_SCALE_TOLERANCE = Decimal("1e-3")
"""Tolerancia relativa para decidir qué escala reconstruye `value`.

Deliberadamente holgada. `value` es la versión REDONDEADA del precio (el propio nombre
`full_accuracy_value` lo dice), así que exigir que coincidan al bit descarta precisamente
el caso que interesa: el campo trae más dígitos que `value`, no los mismos. Con 1e-6 el
ejemplo de la doc `value=189.42` / `full_accuracy_value="189.4217"` se rechazaba y se
perdía la precisión extra.

Holgada no significa ambigua: las dos escalas candidatas se llevan 18 órdenes de magnitud,
así que ninguna tolerancia razonable puede confundirlas. Lo que esto filtra es el caso de
un campo que no cuadra con `value` bajo NINGUNA escala, y ahí manda `value`.
"""


def build_subscribe_message(settings: Settings) -> str:
    """Mensaje de suscripción de RTDS. **Sin `filters`** — ver el punto 1 del módulo.

    Los cuatro tópicos van en un solo mensaje. El filtrado por símbolo ocurre después, en
    `handle_message`, sobre lo que llega.
    """
    subscriptions: list[dict[str, str]] = [{"topic": TOPIC_CHAINLINK, "type": "update"}]
    for window in settings.twap_windows_s:
        topic = _WINDOW_TOPIC.get(window)
        if topic is None:
            raise ValueError(f"RTDS solo admite ventanas TWAP de 30 o 60 s, no {window}")
        subscriptions.append({"topic": topic, "type": "update"})
    subscriptions.append({"topic": TOPIC_BINANCE, "type": "update"})
    return json.dumps({"action": "subscribe", "subscriptions": subscriptions})


def handle_message(raw: str, state: FeedState, settings: Settings) -> None:
    """Enruta un mensaje de RTDS a `state`. Función pura salvo por la escritura en `state`.

    Ignora en silencio lo que no reconoce (frames vacíos, `PONG`, tópicos no suscritos):
    un formato inesperado no debe reventar la conexión de un grabador que tiene que
    aguantar 14 días.
    """
    message = loads_decimal(raw)
    if not isinstance(message, dict):
        return
    topic = message.get("topic")
    body = message.get("payload")
    if not isinstance(topic, str) or not isinstance(body, dict):
        return

    feed = _feed_for(topic, body.get("symbol"), settings)
    if feed is None:
        return

    value = _extract_value(body)
    if value is None or value <= 0:
        return

    state.set_price(feed, value)


def _feed_for(topic: str, symbol: object, settings: Settings) -> str | None:
    """Feed destino de un mensaje, a partir del tópico Y del símbolo.

    El símbolo hace dos trabajos aquí, y los dos son obligatorios:

    - **Filtra.** Sin `filters` en la suscripción llegan todos los símbolos del mercado
      (eth, sol, doge…). Lo que no es nuestro se descarta aquí.
    - **Desambigua.** `crypto_prices_chainlink` puede responder bajo el tópico
      `crypto_prices`, el mismo que el relay de Binance. Sin mirar el símbolo, el precio
      del oráculo aterrizaría en el feed de Binance y el basis de §2.1.b saldría
      idénticamente 1: el sesgo entero, y sin ningún síntoma que lo delate.
    """
    if not isinstance(symbol, str):
        return None
    normalized = symbol.strip().lower()
    is_oracle = normalized == settings.chainlink_symbol.lower()

    twap = _TWAP_TOPIC_TO_FEED.get(topic)
    if twap is not None:
        return twap if is_oracle else None
    if topic == TOPIC_CHAINLINK:
        return FEED_CHAINLINK if is_oracle else None
    if topic == TOPIC_BINANCE:
        if is_oracle:
            return FEED_CHAINLINK  # el oráculo bajo el tópico del relay: ver el módulo
        if normalized == settings.binance_symbol.lower():
            return FEED_BINANCE_RELAY
    return None


async def subscribe(ws: ClientConnection, settings: Settings) -> None:
    await ws.send(build_subscribe_message(settings))


async def run(state: FeedState, settings: Settings) -> None:
    """Tarea de larga duración: mantiene RTDS vivo y volcando a `state`."""

    async def on_connect(ws: ClientConnection) -> None:
        await subscribe(ws, settings)

    await run_feed_forever(
        name=FEED_NAME,
        url=settings.rtds_url,
        on_connect=on_connect,
        on_message=lambda raw: handle_message(raw, state, settings),
        state=state,
        text_ping=TEXT_PING,
        text_ping_interval_s=settings.rtds_text_ping_interval_s,
        open_timeout_s=settings.ws_open_timeout_s,
        recv_timeout_s=settings.ws_recv_timeout_s,
        backoff=BackoffPolicy(settings.ws_backoff_base_s, settings.ws_backoff_max_s),
    )


def _extract_value(body: dict[str, Any]) -> Decimal | None:
    """Precio del payload, sea cual sea de las dos formas que manda el servidor.

    - `type: "update"` → payload plano con `value`.
    - `type: "subscribe"` → payload con `data`, un array de puntos a 1 s; interesa el
      último, que es el más reciente y está al día hasta el instante de la conexión.

    El array llega ordenado por `timestamp` ascendente, pero se toma el máximo por
    timestamp en vez del último elemento: el orden no está documentado y confiar en él
    dejaría el ancla de liquidación colgando de un detalle que nadie prometió.
    """
    data = body.get("data")
    if isinstance(data, list):
        points = [p for p in data if isinstance(p, dict)]
        if not points:
            return None
        newest = max(points, key=lambda p: _timestamp_of(p))
        return _value_of(newest)
    return _value_of(body)


def _timestamp_of(point: dict[str, Any]) -> Decimal:
    ts = as_decimal(point.get("timestamp"))
    return ts if ts is not None else Decimal(0)


def _value_of(body: dict[str, Any]) -> Decimal | None:
    """Precio de un payload plano como `Decimal` exacto.

    El JSON se parsea con `parse_float=Decimal`, así que `value` conserva exactamente los
    dígitos que mandó el servidor: nunca pasa por un `float` intermedio. Eso importa
    porque §4 exige `Decimal` en `feeds/` y este número es el ancla de liquidación.
    """
    value = as_decimal(body.get("value"))
    if value is None or value <= 0:
        return value

    full = as_decimal(body.get("full_accuracy_value"))
    if full is None or full <= 0:
        return value

    for scale in _FULL_ACCURACY_SCALES:
        candidate = full / scale
        if abs(candidate - value) <= _SCALE_TOLERANCE * value:
            return candidate
    return value
