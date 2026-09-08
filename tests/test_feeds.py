"""Tests de los feeds sin tocar la red: parseo, enrutado, staleness y backoff.

Todo lo que decide algo en `feeds/` es una función pura sobre el texto del mensaje, y eso
es deliberado: la parte que no se puede testear sin red (el socket) no toma ninguna
decisión, solo transporta. Aquí se comprueba lo que sí decide.

El invariante que más se vigila es el de §4: en `feeds/` todo precio es `Decimal` y ningún
valor pasa por `float` en el camino. Un `float` intermedio no rompe ningún test de forma
evidente — simplemente redondea el ancla de liquidación y nadie se entera.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from decimal import Decimal

import pytest
from websockets.asyncio.server import ServerConnection, serve

from polybtc.config import Settings
from polybtc.feeds import book, rtds, spot
from polybtc.feeds.parsing import as_decimal, loads_decimal
from polybtc.feeds.state import (
    FEED_BINANCE_DIRECT,
    FEED_BINANCE_RELAY,
    FEED_CHAINLINK,
    FEED_TWAP30,
    FEED_TWAP60,
    FeedState,
    Quote,
    book_feed,
)
from polybtc.feeds.ws import BackoffPolicy, run_feed_forever


@pytest.fixture
def settings() -> Settings:
    return Settings()


@pytest.fixture
def state() -> FeedState:
    return FeedState(staleness_s=3.0)


# ---------------------------------------------------------------------------
# Parseo: la frontera Decimal empieza en el propio json.loads
# ---------------------------------------------------------------------------


def test_loads_decimal_no_pasa_por_float() -> None:
    """El valor debe conservar los dígitos del mensaje, no los del float más cercano."""
    raw = '{"value": 65000.100000000000001}'
    parsed = loads_decimal(raw)
    assert isinstance(parsed, dict)
    assert parsed["value"] == Decimal("65000.100000000000001")
    # Vía float ese literal se redondea; el Decimal exacto no coincide con él.
    assert parsed["value"] != Decimal(str(float("65000.100000000000001")))


def test_loads_decimal_devuelve_none_en_frames_de_control() -> None:
    """`PONG` no es JSON y no debe reventar un grabador que corre 14 días."""
    assert loads_decimal("PONG") is None
    assert loads_decimal("") is None
    assert loads_decimal("{no es json}") is None


def test_as_decimal_rechaza_float_y_bool() -> None:
    """Un `float` aquí significa que alguien parseó mal río arriba: no se blanquea."""
    assert as_decimal(1.5) is None
    assert as_decimal(True) is None
    assert as_decimal("12.25") == Decimal("12.25")
    assert as_decimal(3) == Decimal(3)


# ---------------------------------------------------------------------------
# RTDS
# ---------------------------------------------------------------------------


def test_subscribe_incluye_los_cuatro_topicos(settings: Settings) -> None:
    """Los cuatro van por UNA conexión: es lo que permite el cross-check de §2.1.b."""
    message = json.loads(rtds.build_subscribe_message(settings))

    assert message["action"] == "subscribe"
    topics = [sub["topic"] for sub in message["subscriptions"]]
    assert topics == [
        rtds.TOPIC_CHAINLINK,
        rtds.TOPIC_TWAP_30,
        rtds.TOPIC_TWAP_60,
        rtds.TOPIC_BINANCE,
    ]


def test_la_suscripcion_no_lleva_filters(settings: Settings) -> None:
    """El campo `filters` apaga el stream en vivo. Es el hallazgo central del módulo.

    Medido contra el servidor el 2026-09-02: con `filters` la suscripción se acusa y luego
    no llega ni un update; sin `filters` llegan ~1/s de forma sostenida. Añadirlo "para
    ahorrar ancho de banda" es la regresión más fácil de cometer aquí y la más difícil de
    diagnosticar, porque no produce ningún error: produce un feed permanentemente stale.
    """
    subs = json.loads(rtds.build_subscribe_message(settings))["subscriptions"]

    assert [sub["topic"] for sub in subs] == [
        rtds.TOPIC_CHAINLINK,
        rtds.TOPIC_TWAP_30,
        rtds.TOPIC_TWAP_60,
        rtds.TOPIC_BINANCE,
    ]
    for sub in subs:
        assert "filters" not in sub, f"{sub['topic']}: `filters` deja el feed sin updates"


def test_subscribe_rechaza_ventana_no_soportada() -> None:
    """RTDS solo publica 30 y 60. Una ventana inventada debe fallar aquí, no en silencio."""
    with pytest.raises(ValueError, match="30 o 60"):
        rtds.build_subscribe_message(Settings(twap_windows_s=(45,)))


@pytest.mark.parametrize(
    ("topic", "symbol", "feed"),
    [
        (rtds.TOPIC_CHAINLINK, "btc/usd", FEED_CHAINLINK),
        # El oráculo puede llegar TAMBIÉN bajo `crypto_prices`, el tópico del relay:
        # solo el símbolo los distingue.
        (rtds.TOPIC_BINANCE, "btc/usd", FEED_CHAINLINK),
        (rtds.TOPIC_BINANCE, "btcusdt", FEED_BINANCE_RELAY),
        (rtds.TOPIC_TWAP_30, "btc/usd", FEED_TWAP30),
        (rtds.TOPIC_TWAP_60, "btc/usd", FEED_TWAP60),
    ],
)
def test_el_enrutado_usa_el_simbolo_no_solo_el_topico(
    state: FeedState, settings: Settings, topic: str, symbol: str, feed: str
) -> None:
    """Enrutar solo por tópico metería el oráculo en el feed de Binance.

    Y entonces el basis de §2.1.b saldría idénticamente 1: el sesgo que ese estimador
    existe para corregir volvería entero, sin ningún síntoma visible.
    """
    raw = json.dumps(
        {
            "topic": topic,
            "type": "update",
            "timestamp": 1785178800123,
            "payload": {"symbol": symbol, "value": 65000.5, "timestamp": 1785178800000},
        }
    )
    rtds.handle_message(raw, state, settings)

    assert state.prices == {feed: Decimal("65000.5")}
    assert state.age_s(feed) is not None


def test_backfill_de_la_suscripcion_toma_el_punto_mas_reciente(
    state: FeedState, settings: Settings
) -> None:
    """Al suscribirse llega `type: "subscribe"` con `payload.data`, que la doc no describe.

    Se toma el punto de mayor `timestamp`, no el último del array: el orden no está
    documentado y el ancla de liquidación no debe depender de un detalle que nadie promete.
    """
    raw = json.dumps(
        {
            "topic": rtds.TOPIC_TWAP_30,
            "type": "subscribe",
            "payload": {
                "symbol": "btc/usd",
                "window_s": 30,
                "data": [
                    {"timestamp": 1788388154000, "value": 76999.25},
                    {"timestamp": 1788388156000, "value": 77001.10},
                    {"timestamp": 1788388155000, "value": 77000.00},
                ],
            },
        }
    )
    rtds.handle_message(raw, state, settings)

    assert state.prices[FEED_TWAP30] == Decimal("77001.10")


def test_backfill_vacio_no_escribe_nada(state: FeedState, settings: Settings) -> None:
    raw = json.dumps(
        {
            "topic": rtds.TOPIC_TWAP_30,
            "type": "subscribe",
            "payload": {"data": [], "symbol": "btc/usd"},
        }
    )
    rtds.handle_message(raw, state, settings)

    assert state.prices == {}


def test_full_accuracy_value_escalado_gana_al_value(state: FeedState, settings: Settings) -> None:
    """La escala no se asume: se elige la que reconstruye `value`.

    La doc muestra `full_accuracy_value` escalado a 18 decimales en el TWAP y SIN escalar
    en otro tópico, sin decir cuál aplica a cuál. Adivinar sería un error de 18 órdenes de
    magnitud en el ancla de liquidación.
    """
    raw = json.dumps(
        {
            "topic": rtds.TOPIC_TWAP_30,
            "payload": {
                "symbol": "btc/usd",
                "value": 65000.5,
                "full_accuracy_value": "65000512345678901234567",
                "window_s": 30,
            },
        }
    )
    rtds.handle_message(raw, state, settings)

    assert state.prices[FEED_TWAP30] == Decimal("65000.512345678901234567")


def test_full_accuracy_value_sin_escalar_tambien_se_reconoce(
    state: FeedState, settings: Settings
) -> None:
    raw = json.dumps(
        {
            "topic": rtds.TOPIC_BINANCE,
            "payload": {"symbol": "btc/usd", "value": 189.42, "full_accuracy_value": "189.4217"},
        }
    )
    rtds.handle_message(raw, state, settings)

    assert state.prices[FEED_CHAINLINK] == Decimal("189.4217")


def test_full_accuracy_value_incoherente_se_descarta(state: FeedState, settings: Settings) -> None:
    """Si ninguna escala reconstruye `value`, manda `value`. Nunca se inventa un factor."""
    raw = json.dumps(
        {
            "topic": rtds.TOPIC_BINANCE,
            "payload": {"symbol": "btc/usd", "value": 65000.5, "full_accuracy_value": "7"},
        }
    )
    rtds.handle_message(raw, state, settings)

    assert state.prices[FEED_CHAINLINK] == Decimal("65000.5")


@pytest.mark.parametrize(
    "raw",
    [
        "PONG",
        "",  # el servidor manda un frame vacío al conectar
        '{"topic": "comments", "payload": {"body": "hola"}}',
        # Sin `filters` llegan todos los símbolos del mercado: lo ajeno se descarta aquí.
        '{"topic": "crypto_prices", "payload": {"symbol": "ethusdt", "value": 3000}}',
        '{"topic": "crypto_prices_chainlink", "payload": {"symbol": "sol/usd", "value": 99.8}}',
        '{"topic": "crypto_prices_twap_thirty", "payload": {"symbol": "doge/usd", "value": 0.08}}',
        # Sin símbolo no se puede saber si es el oráculo o el relay: se descarta.
        '{"topic": "crypto_prices", "payload": {"value": 65000.5}}',
        '{"topic": "crypto_prices", "payload": {"symbol": "btc/usd", "value": 0}}',
        '{"topic": "crypto_prices", "payload": {"symbol": "btc/usd", "value": -1}}',
        '{"topic": "crypto_prices_twap_thirty"}',
        '{"topic": "crypto_prices_twap_thirty", "payload": {}}',
    ],
)
def test_mensajes_ignorables_no_escriben_ni_lanzan(
    state: FeedState, settings: Settings, raw: str
) -> None:
    """Un formato inesperado no debe cortar la conexión ni ensuciar el estado."""
    rtds.handle_message(raw, state, settings)

    assert state.prices == {}


# ---------------------------------------------------------------------------
# Binance directo
# ---------------------------------------------------------------------------


def test_book_ticker_expone_el_mid_no_el_ultimo_trade(state: FeedState) -> None:
    """§3.4: el insumo de σ es el mid del top of book."""
    raw = json.dumps({"u": 400900217, "s": "BTCUSDT", "b": "25.30", "B": "31.2", "a": "25.40"})
    spot.handle_message(raw, state)

    assert state.prices[FEED_BINANCE_DIRECT] == Decimal("25.35")


def test_book_ticker_acepta_precio_como_numero_y_como_cadena() -> None:
    """Binance ha publicado ambos formatos; los dos deben dar el mismo `Decimal` exacto."""
    como_numero = spot.parse_book_ticker('{"b": 25.3519, "a": 25.3652}')
    como_cadena = spot.parse_book_ticker('{"b": "25.3519", "a": "25.3652"}')

    assert como_numero == como_cadena == Quote(bid=Decimal("25.3519"), ask=Decimal("25.3652"))


@pytest.mark.parametrize(
    "raw",
    [
        '{"b": "25.40", "a": "25.30"}',  # cruzado
        '{"b": "25.35", "a": "25.35"}',  # bid == ask
        '{"b": "0", "a": "25.30"}',
        '{"s": "BTCUSDT"}',
        "ping",
    ],
)
def test_book_ticker_descarta_mensajes_incoherentes(raw: str) -> None:
    """Un mid sobre un book cruzado envenenaría σ y el basis a la vez."""
    assert spot.parse_book_ticker(raw) is None


def test_stream_url_lleva_el_simbolo_en_minusculas() -> None:
    url = spot.stream_url(Settings(binance_symbol="BTCUSDT"))

    assert url.endswith("/btcusdt@bookTicker")


# ---------------------------------------------------------------------------
# Book del CLOB
# ---------------------------------------------------------------------------


def test_book_no_arranca_sin_token_ids(settings: Settings) -> None:
    """El mercado objetivo es una decisión abierta de §8: no se asume ninguno."""
    with pytest.raises(ValueError, match="token_ids"):
        book.build_subscribe_message(settings)


def test_subscribe_del_book_pide_best_bid_ask() -> None:
    """`best_bid_ask` SOLO llega con `custom_feature_enabled`; sin él hay que derivarlo."""
    message = json.loads(book.build_subscribe_message(Settings(token_ids=("0xabc",))))

    assert message["assets_ids"] == ["0xabc"]
    assert message["type"] == "market"
    assert message["custom_feature_enabled"] is True
    assert message["initial_dump"] is True


def test_snapshot_no_asume_el_orden_de_los_niveles(state: FeedState) -> None:
    """La doc no promete orden en `bids`/`asks`, así que se toma max/min, no un extremo."""
    raw = json.dumps(
        {
            "event_type": "book",
            "asset_id": "0xabc",
            "bids": [
                {"price": "0.480", "size": "100"},
                {"price": "0.495", "size": "50"},
                {"price": "0.470", "size": "10"},
            ],
            "asks": [
                {"price": "0.530", "size": "80"},
                {"price": "0.505", "size": "20"},
            ],
        }
    )
    book.handle_message(raw, state)

    assert state.book("0xabc") == Quote(bid=Decimal("0.495"), ask=Decimal("0.505"))


def test_price_change_usa_la_ultima_entrada(state: FeedState) -> None:
    """Son deltas en orden: la posterior manda."""
    raw = json.dumps(
        {
            "event_type": "price_change",
            "price_changes": [
                {"asset_id": "0xabc", "best_bid": "0.400", "best_ask": "0.410"},
                {"asset_id": "0xabc", "best_bid": "0.420", "best_ask": "0.430"},
            ],
        }
    )
    book.handle_message(raw, state)

    assert state.book("0xabc") == Quote(bid=Decimal("0.420"), ask=Decimal("0.430"))


def test_tick_size_change_reemplaza_el_valor_guardado(state: FeedState) -> None:
    """§4: el CLOB rechaza órdenes con tick obsoleto, y el rechazo no es descriptivo."""
    state.set_tick_size("0xabc", Decimal("0.01"))
    raw = json.dumps(
        {
            "event_type": "tick_size_change",
            "asset_id": "0xabc",
            "old_tick_size": "0.01",
            "new_tick_size": "0.001",
        }
    )
    book.handle_message(raw, state)

    assert state.tick_size("0xabc") == Decimal("0.001")


def test_book_acepta_un_array_de_eventos(state: FeedState) -> None:
    """El canal puede mandar varios eventos en un frame."""
    raw = json.dumps(
        [
            {"event_type": "tick_size_change", "asset_id": "0xabc", "new_tick_size": "0.001"},
            {
                "event_type": "best_bid_ask",
                "asset_id": "0xabc",
                "best_bid": "0.51",
                "best_ask": "0.52",
            },
        ]
    )
    book.handle_message(raw, state)

    assert state.tick_size("0xabc") == Decimal("0.001")
    assert state.book("0xabc") == Quote(bid=Decimal("0.51"), ask=Decimal("0.52"))


def test_last_trade_price_no_toca_el_book(state: FeedState) -> None:
    """§3.4: el último trade lleva bid-ask bounce y no es insumo de nada."""
    raw = json.dumps(
        {"event_type": "last_trade_price", "asset_id": "0xabc", "price": "0.55", "size": "10"}
    )
    book.handle_message(raw, state)

    assert state.books == {}


# ---------------------------------------------------------------------------
# Staleness (§5.2) y backoff
# ---------------------------------------------------------------------------


def test_feed_sin_datos_cuenta_como_stale(state: FeedState) -> None:
    """`None` no es "fresco": es "no hay dato", y con eso no se cotiza."""
    assert state.age_s(FEED_CHAINLINK) is None
    assert state.is_stale(FEED_CHAINLINK)


def test_staleness_dispara_en_el_umbral(state: FeedState) -> None:
    state.set_price(FEED_CHAINLINK, Decimal("65000"), now=100.0)

    assert not state.is_stale(FEED_CHAINLINK, now=102.9)
    assert state.is_stale(FEED_CHAINLINK, now=103.1)


def test_stale_feeds_lista_solo_los_requeridos(state: FeedState) -> None:
    state.set_price(FEED_CHAINLINK, Decimal("65000"), now=100.0)
    state.set_book("0xabc", Quote(bid=Decimal("0.49"), ask=Decimal("0.51")), now=100.0)

    required = (FEED_CHAINLINK, FEED_TWAP30, book_feed("0xabc"))
    assert state.stale_feeds(required, now=101.0) == [FEED_TWAP30]


def test_mid_del_quote_es_exacto() -> None:
    """Sin `float` de por medio: el mid de dos ticks es un `Decimal` exacto (§4)."""
    quote = Quote(bid=Decimal("0.481"), ask=Decimal("0.482"))

    assert quote.mid == Decimal("0.4815")
    assert quote.spread == Decimal("0.001")


def test_precio_no_positivo_es_un_error_del_llamante(state: FeedState) -> None:
    with pytest.raises(ValueError, match="no positivo"):
        state.set_price(FEED_CHAINLINK, Decimal(0))


def test_backoff_esta_acotado_y_crece() -> None:
    """Full jitter: siempre en [0, cap], con el techo creciendo hasta `max_delay_s`."""
    policy = BackoffPolicy(base_s=0.5, max_delay_s=30.0)

    assert policy.delay_for(0, rand=lambda: 1.0) == 0.5
    assert policy.delay_for(3, rand=lambda: 1.0) == 4.0
    assert policy.delay_for(20, rand=lambda: 1.0) == 30.0  # capado
    assert policy.delay_for(20, rand=lambda: 0.0) == 0.0  # el jitter llega hasta cero


def test_backoff_jitter_desincroniza_las_reconexiones() -> None:
    """Sin jitter, tres feeds caídos a la vez reconectarían en el mismo instante siempre."""
    policy = BackoffPolicy(base_s=1.0, max_delay_s=30.0)
    delays = {policy.delay_for(5) for _ in range(50)}

    assert len(delays) > 1
    assert all(0.0 <= d <= 30.0 for d in delays)


def test_reconexiones_se_contabilizan(state: FeedState) -> None:
    """El contador es panel de la Fase 0: huecos en los datos invalidan el análisis (§6)."""
    state.note_reconnect("rtds")
    state.note_reconnect("rtds")

    assert state.reconnects == {"rtds": 2}


# ---------------------------------------------------------------------------
# Reconexión contra un servidor local que se cae
# ---------------------------------------------------------------------------


async def test_el_feed_se_recupera_solo_de_una_caida() -> None:
    """El criterio de la Fase 0: 14 días grabando sin nadie delante para reiniciarlo.

    Un corte de red real es la prueba de fuego y se hace a mano. Esto cubre la parte que
    sí se puede automatizar: que tras una caída se reconecte, se re-suscriba y siga
    entregando. Sin esto, un fallo de reconexión solo se descubre al mirar los datos dos
    semanas después, cuando ya no hay nada que salvar.
    """
    received: list[str] = []
    enough = asyncio.Event()
    connections = 0
    subscribes = 0

    async def handler(ws: ServerConnection) -> None:
        nonlocal connections
        connections += 1
        await ws.send(json.dumps({"n": connections}))
        await ws.close()  # el servidor tira la conexión a propósito

    async def on_connect(ws: object) -> None:
        nonlocal subscribes
        subscribes += 1

    def on_message(raw: str) -> None:
        received.append(raw)
        if len(received) >= 3:
            enough.set()

    state = FeedState(staleness_s=3.0)
    async with serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        task = asyncio.create_task(
            run_feed_forever(
                name="prueba",
                url=f"ws://127.0.0.1:{port}",
                on_connect=on_connect,
                on_message=on_message,
                state=state,
                backoff=BackoffPolicy(base_s=0.001, max_delay_s=0.01),
            )
        )
        try:
            await asyncio.wait_for(enough.wait(), timeout=5.0)
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    assert connections >= 3, "no se volvió a conectar tras la caída"
    assert subscribes == connections, "cada reconexión debe re-suscribirse"
    assert state.reconnects["prueba"] >= 2, "las reconexiones deben quedar contabilizadas"


async def test_cancelar_la_tarea_para_el_feed() -> None:
    """Un apagado ordenado debe poder parar el feed: `CancelledError` no se traga."""
    connected = asyncio.Event()

    async def handler(ws: ServerConnection) -> None:
        connected.set()
        async for _ in ws:  # termina solo cuando el cliente se va
            pass

    state = FeedState()
    async with serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        task = asyncio.create_task(
            run_feed_forever(
                name="prueba",
                url=f"ws://127.0.0.1:{port}",
                on_connect=_noop_connect,
                on_message=lambda _raw: None,
                state=state,
            )
        )
        await asyncio.wait_for(connected.wait(), timeout=5.0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


async def _noop_connect(_ws: object) -> None:
    return None


def test_la_suspension_del_sistema_no_finge_frescura() -> None:
    """En macOS `time.monotonic()` no cuenta el tiempo suspendido.

    Reproduce lo medido en una corrida real: la máquina durmió 108 s de reloj de pared y
    el monótono avanzó 1 s. Con solo el monótono, el feed figuraba con 2 s de edad y el
    halt de §5.2 habría dejado cotizar sobre datos de casi dos minutos.
    """
    state = FeedState(staleness_s=3.0)
    state.set_price(FEED_CHAINLINK, Decimal("78513.73"), now=1_000.0, wall=1_700_000_000.0)

    # Un segundo de monótono, 108 de pared: exactamente el patrón de la suspensión.
    edad = state.age_s(FEED_CHAINLINK, now=1_001.0, wall=1_700_000_108.0)

    assert edad == pytest.approx(108.0)
    assert state.is_stale(FEED_CHAINLINK, now=1_001.0, wall=1_700_000_108.0)


def test_un_salto_de_ntp_hacia_atras_no_borra_la_edad() -> None:
    """El reloj de pared solo tampoco vale: NTP lo mueve y fabricaría frescura.

    El máximo de los dos relojes se equivoca siempre hacia declarar stale de más, que es
    el lado seguro.
    """
    state = FeedState(staleness_s=3.0)
    state.set_price(FEED_CHAINLINK, Decimal("78513.73"), now=1_000.0, wall=1_700_000_000.0)

    # 10 s de monótono, pero NTP retrasó el reloj de pared 60 s.
    edad = state.age_s(FEED_CHAINLINK, now=1_010.0, wall=1_699_999_940.0)

    assert edad == pytest.approx(10.0)
    assert state.is_stale(FEED_CHAINLINK, now=1_010.0, wall=1_699_999_940.0)


def test_los_dos_outcomes_no_comparten_libro(state: FeedState) -> None:
    """Una binaria son DOS instrumentos con precios complementarios.

    Antes de indexar por `asset_id` esto se veía en vivo como `0.02/0.03` alternando con
    `0.97/0.98` en la misma casilla, un segundo sí y otro no. No hay error, no hay excepción
    y el consumidor no puede notar que está leyendo el instrumento equivocado — que en un
    mercado binario es el contrario exacto del que cree.
    """
    raw = json.dumps(
        [
            {
                "event_type": "best_bid_ask",
                "asset_id": "UP",
                "best_bid": "0.97",
                "best_ask": "0.98",
            },
            {
                "event_type": "best_bid_ask",
                "asset_id": "DOWN",
                "best_bid": "0.02",
                "best_ask": "0.03",
            },
        ]
    )
    book.handle_message(raw, state)

    assert state.book("UP") == Quote(bid=Decimal("0.97"), ask=Decimal("0.98"))
    assert state.book("DOWN") == Quote(bid=Decimal("0.02"), ask=Decimal("0.03"))


def test_price_change_reparte_por_token(state: FeedState) -> None:
    """Un mismo frame puede traer deltas de los dos outcomes mezclados."""
    raw = json.dumps(
        {
            "event_type": "price_change",
            "price_changes": [
                {"asset_id": "UP", "best_bid": "0.90", "best_ask": "0.91"},
                {"asset_id": "DOWN", "best_bid": "0.09", "best_ask": "0.10"},
                {"asset_id": "UP", "best_bid": "0.92", "best_ask": "0.93"},
            ],
        }
    )
    book.handle_message(raw, state)

    assert state.book("UP") == Quote(bid=Decimal("0.92"), ask=Decimal("0.93"))
    assert state.book("DOWN") == Quote(bid=Decimal("0.09"), ask=Decimal("0.10"))


def test_un_evento_sin_asset_id_se_descarta(state: FeedState) -> None:
    """Sin `asset_id` no se sabe de qué outcome se habla: adivinarlo es peor que perderlo."""
    raw = json.dumps({"event_type": "best_bid_ask", "best_bid": "0.51", "best_ask": "0.52"})
    book.handle_message(raw, state)

    assert state.books == {}
