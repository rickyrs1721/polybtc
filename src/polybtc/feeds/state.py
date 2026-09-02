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
FEED_BOOK = "book"

ALL_FEEDS: tuple[str, ...] = (
    FEED_CHAINLINK,
    FEED_TWAP30,
    FEED_TWAP60,
    FEED_BINANCE_RELAY,
    FEED_BINANCE_DIRECT,
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

    book: Quote | None = None
    tick_size: Decimal | None = None
    """Solo lo que dijo `tick_size_change` o el snapshot: nunca un valor asumido (§4)."""

    _updated_at: dict[str, float] = field(default_factory=dict)
    reconnects: dict[str, int] = field(default_factory=dict)

    # -- escritura ----------------------------------------------------------------

    def set_price(self, name: str, value: Decimal, *, now: float | None = None) -> None:
        if value <= 0:
            raise ValueError(f"precio no positivo en el feed {name!r}: {value}")
        self.prices[name] = value
        self._updated_at[name] = _now(now)

    def set_book(self, quote: Quote, *, now: float | None = None) -> None:
        self.book = quote
        self._updated_at[FEED_BOOK] = _now(now)

    def set_tick_size(self, tick_size: Decimal) -> None:
        if tick_size <= 0:
            raise ValueError(f"tick_size debe ser positivo: {tick_size}")
        self.tick_size = tick_size

    def note_reconnect(self, name: str) -> None:
        self.reconnects[name] = self.reconnects.get(name, 0) + 1

    # -- lectura ------------------------------------------------------------------

    def age_s(self, name: str, *, now: float | None = None) -> float | None:
        """Segundos desde la última actualización, o `None` si nunca llegó nada.

        `None` no es "fresco": es "no hay dato", y `is_stale` lo trata como stale.
        """
        last = self._updated_at.get(name)
        if last is None:
            return None
        return max(0.0, _now(now) - last)

    def is_stale(self, name: str, *, now: float | None = None) -> bool:
        age = self.age_s(name, now=now)
        return age is None or age > self.staleness_s

    def stale_feeds(self, required: tuple[str, ...], *, now: float | None = None) -> list[str]:
        """Los feeds de `required` que no están frescos. Vacío = se puede cotizar."""
        return [name for name in required if self.is_stale(name, now=now)]


def _now(now: float | None = None) -> float:
    """Reloj monótono. Inyectable porque los tests no deben depender de dormir."""
    return time.monotonic() if now is None else now
