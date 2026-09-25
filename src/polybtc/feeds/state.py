"""Estado compartido de los feeds: último valor, edad y reconexiones de cada uno.

Existe para una sola cosa: que el halt por staleness de §5.2 sea comprobable en un punto
y no reconstruido a mano en cada consumidor. Cotizar con un TWAP de hace 8 segundos es
cómo se pierde dinero rápido, y el único modo de no hacerlo es que la edad sea un dato de
primera clase.

Todo lo que es precio, tamaño o monto es `Decimal` (§4). Las EDADES no lo son: son tiempo
medido con un reloj monótono, no dinero, y `float` es el tipo correcto ahí.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from decimal import Decimal

FEED_CHAINLINK = "chainlink"
FEED_TWAP30 = "twap30"
FEED_TWAP60 = "twap60"
FEED_BINANCE_RELAY = "binance_relay"
FEED_BINANCE_DIRECT = "binance_direct"
FEED_COINBASE = "coinbase"
"""Spot de Coinbase (BTC-USD). Alternativa a Binance por latencia y por denominación.

Se graba SIEMPRE, aunque el modelo use otra fuente: cuál de las dos sirve mejor es una
pregunta que deben contestar los datos de la Fase 0, no un RTT medido una tarde. Es el
mismo patrón que las dos hipótesis de ventana TWAP de §8.
"""

FEED_BOOK_PREFIX = "book:"


def book_feed(asset_id: str) -> str:
    """Clave de staleness del book de UN outcome.

    Cada outcome de la binaria es un instrumento con su propio libro. Meterlos en una sola
    casilla mezcla precios complementarios —medido en vivo: `0.02/0.03` del "Down"
    alternando con `0.97/0.98` del "Up" en la misma línea— y el consumidor no tiene forma
    de notar que está leyendo el instrumento equivocado.
    """
    return f"{FEED_BOOK_PREFIX}{asset_id}"


FEED_BOOK = "book"
"""Solo para listados legibles. La clave real de cada libro la da `book_feed`."""

ALL_FEEDS: tuple[str, ...] = (
    FEED_CHAINLINK,
    FEED_TWAP30,
    FEED_TWAP60,
    FEED_BINANCE_RELAY,
    FEED_BINANCE_DIRECT,
    FEED_COINBASE,
    FEED_BOOK,
)


@dataclass(frozen=True, slots=True)
class Quote:
    """Top of book. El mid de aquí es el insumo de σ (§3.4), nunca el último trade."""

    bid: Decimal
    ask: Decimal

    @property
    def mid(self) -> Decimal:
        return (self.bid + self.ask) / Decimal(2)

    @property
    def spread(self) -> Decimal:
        return self.ask - self.bid


@dataclass(slots=True)
class FeedState:
    """Pizarra compartida entre las tareas de feed y sus consumidores.

    Un solo event loop escribe y lee, así que no hace falta lock. Si algún día hay hilos,
    esta clase es el único sitio que hay que proteger.
    """

    staleness_s: float = 3.0

    prices: dict[str, Decimal] = field(default_factory=dict)
    """Último valor de cada feed de precio puntual, en unidades de su propia fuente."""

    books: dict[str, Quote] = field(default_factory=dict)
    """Top of book por `asset_id` (token del outcome), nunca compartido entre outcomes."""

    tick_sizes: dict[str, Decimal] = field(default_factory=dict)
    """Tick vigente por `asset_id`. Solo lo que dijo el servidor: nunca un valor asumido (§4)."""

    _updated_at: dict[str, float] = field(default_factory=dict)
    """Instante monótono de la última actualización."""

    _updated_wall: dict[str, float] = field(default_factory=dict)
    """Instante de reloj de pared de la misma actualización. NO es redundante: ver `age_s`."""

    reconnects: dict[str, int] = field(default_factory=dict)

    # -- escritura ----------------------------------------------------------------

    def set_price(
        self, name: str, value: Decimal, *, now: float | None = None, wall: float | None = None
    ) -> None:
        if value <= 0:
            raise ValueError(f"precio no positivo en el feed {name!r}: {value}")
        self.prices[name] = value
        self._stamp(name, now, wall)

    def set_book(
        self,
        asset_id: str,
        quote: Quote,
        *,
        now: float | None = None,
        wall: float | None = None,
    ) -> None:
        self.books[asset_id] = quote
        self._stamp(book_feed(asset_id), now, wall)

    def book(self, asset_id: str) -> Quote | None:
        return self.books.get(asset_id)

    def forget_book(self, asset_id: str) -> None:
        """Olvida por completo un libro: su quote, su tick y su edad.

        Los token ids rotan con cada época (§8.1), así que en 14 días de Fase 0 pasan por
        aquí unos 2.700 libros. Sin olvidarlos no solo crecen los diccionarios: quedan
        edades vivas de instrumentos que ya no existen, y `stale_feeds` sobre una lista
        construida a partir de `books` empezaría a reportar como stale mercados muertos.
        """
        feed = book_feed(asset_id)
        self.books.pop(asset_id, None)
        self.tick_sizes.pop(asset_id, None)
        self._updated_at.pop(feed, None)
        self._updated_wall.pop(feed, None)

    def _stamp(self, name: str, now: float | None, wall: float | None) -> None:
        self._updated_at[name] = _now(now)
        self._updated_wall[name] = _wall(now if wall is None else wall)

    def set_tick_size(self, asset_id: str, tick_size: Decimal) -> None:
        if tick_size <= 0:
            raise ValueError(f"tick_size debe ser positivo: {tick_size}")
        self.tick_sizes[asset_id] = tick_size

    def tick_size(self, asset_id: str) -> Decimal | None:
        return self.tick_sizes.get(asset_id)

    def note_reconnect(self, name: str) -> None:
        self.reconnects[name] = self.reconnects.get(name, 0) + 1

    # -- lectura ------------------------------------------------------------------

    def age_s(
        self, name: str, *, now: float | None = None, wall: float | None = None
    ) -> float | None:
        """Segundos desde la última actualización, o `None` si nunca llegó nada.

        `None` no es "fresco": es "no hay dato", y `is_stale` lo trata como stale.

        **Se toma el máximo entre el reloj monótono y el de pared, y no es paranoia:**
        en macOS `time.monotonic()` NO cuenta el tiempo que la máquina pasa suspendida.
        Medido en una corrida de una hora: la máquina se durmió 108 s y al despertar el
        monótono había avanzado 1 s, así que todos los feeds figuraban con 2 s de edad
        cuando los datos tenían casi dos minutos. Con solo el monótono, el halt por
        staleness de §5.2 —la salvaguarda que existe justo para no cotizar sobre datos
        viejos— habría dado luz verde sobre datos de hace dos minutos.

        El de pared solo tampoco vale: un ajuste de NTP lo mueve hacia atrás y fabricaría
        frescura de la nada. El máximo de los dos se equivoca siempre hacia el lado
        seguro, que es declarar stale de más.
        """
        last = self._updated_at.get(name)
        if last is None:
            return None
        by_monotonic = _now(now) - last
        by_wall = _wall(now if wall is None else wall) - self._updated_wall.get(name, last)
        return max(0.0, by_monotonic, by_wall)

    def is_stale(self, name: str, *, now: float | None = None, wall: float | None = None) -> bool:
        age = self.age_s(name, now=now, wall=wall)
        return age is None or age > self.staleness_s

    def stale_feeds(
        self, required: tuple[str, ...], *, now: float | None = None, wall: float | None = None
    ) -> list[str]:
        """Los feeds de `required` que no están frescos. Vacío = se puede cotizar."""
        return [name for name in required if self.is_stale(name, now=now, wall=wall)]


def _now(now: float | None = None) -> float:
    """Reloj monótono. Inyectable porque los tests no deben depender de dormir."""
    return time.monotonic() if now is None else now


def _wall(wall: float | None = None) -> float:
    """Reloj de pared. Sí cuenta la suspensión del sistema; ver `FeedState.age_s`."""
    return time.time() if wall is None else wall
