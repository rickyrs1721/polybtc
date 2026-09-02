"""Una línea por segundo con el estado de todos los feeds. Solo lectura, sin credenciales.

Es la prueba de fuego del Sprint de feeds: se deja corriendo una hora, se corta el WiFi 30
segundos y se mira que las edades se disparen y vuelvan solas. Si no reconecta, no sirve
para los 14 días de la Fase 0 (§6) — un hueco en los datos envenena el análisis.

También es el primer sitio donde se ve el basis de §2.1.b con datos reales, que es la
pregunta 5 de la Fase 0 y el prerrequisito de todas las demás.

Uso:
    uv run python scripts/tail_feeds.py
    POLYBTC_TOKEN_IDS='["0x…"]' uv run python scripts/tail_feeds.py   # con book
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
from datetime import UTC, datetime
from decimal import Decimal

from polybtc.config import Settings
from polybtc.feeds import book, rtds, spot
from polybtc.feeds.state import (
    FEED_BINANCE_DIRECT,
    FEED_BINANCE_RELAY,
    FEED_BOOK,
    FEED_CHAINLINK,
    FEED_TWAP30,
    FEED_TWAP60,
    FeedState,
)
from polybtc.model.basis import BasisEstimator

TICK_S = 1.0

HEADER = (
    f"{'hora':>8}  {'chainlink':>11} {'twap30':>11} {'twap60':>11} "
    f"{'bnc_dir':>11} {'bnc_rly':>11}  {'basis$':>8} {'n':>4}  "
    f"{'bid':>6} {'ask':>6}  edades(s)"
)


async def main() -> None:
    settings = Settings()
    state = FeedState(staleness_s=settings.staleness_s)
    basis = BasisEstimator(
        half_life_s=settings.basis_half_life_s,
        clamp=settings.basis_clamp,
        min_samples=settings.basis_min_samples,
    )

    tasks = [
        asyncio.create_task(rtds.run(state, settings), name="rtds"),
        asyncio.create_task(spot.run(state, settings), name="spot"),
    ]
    if settings.token_ids:
        tasks.append(asyncio.create_task(book.run(state, settings), name="book"))
    else:
        print("· sin POLYBTC_TOKEN_IDS: el feed de book no arranca (decisión abierta de §8)")

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)

    try:
        await _print_loop(state, basis, stop)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def _print_loop(state: FeedState, basis: BasisEstimator, stop: asyncio.Event) -> None:
    tick = 0
    while not stop.is_set():
        if tick % 25 == 0:
            print(HEADER)
        _feed_basis(state, basis)
        print(_render(state, basis), flush=True)
        tick += 1
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=TICK_S)


def _feed_basis(state: FeedState, basis: BasisEstimator) -> None:
    """Alimenta el estimador solo con una pareja fresca de ambos lados.

    Mezclar un Chainlink de hace 5 segundos con un Binance de hace 20 ms no mide el basis:
    mide el rezago del TWAP. Por eso se exige que ninguno de los dos esté stale.
    """
    if state.is_stale(FEED_CHAINLINK) or state.is_stale(FEED_BINANCE_DIRECT):
        return
    chainlink = state.prices.get(FEED_CHAINLINK)
    binance = state.prices.get(FEED_BINANCE_DIRECT)
    if chainlink is None or binance is None:
        return
    # Cruce de `feeds/` (Decimal) a `model/` (float): §4 pone float en todo el modelo.
    basis.update(asyncio.get_running_loop().time(), float(chainlink), float(binance))


def _render(state: FeedState, basis: BasisEstimator) -> str:
    now = datetime.now(UTC).strftime("%H:%M:%S")
    binance = state.prices.get(FEED_BINANCE_DIRECT)

    # El basis se muestra en DÓLARES, no en logs: $30 sin corregir valen 6.66¢ de fair a
    # τ=300 s. En logs el número es ilegible y nadie detecta que se está moviendo.
    if binance is not None and basis.samples > 0:
        basis_usd = f"{float(binance) * (basis.basis_ratio - 1.0):+8.2f}"
    else:
        basis_usd = "       ·"
    if not basis.ready:
        basis_usd = basis_usd.strip().rjust(7) + "?"  # `?` = aún no converge → no cotizar

    quote = state.book
    bid = _money(quote.bid, 3) if quote else "     ·"
    ask = _money(quote.ask, 3) if quote else "     ·"

    ages = " ".join(
        f"{label}={_age(state, feed)}"
        for label, feed in (
            ("cl", FEED_CHAINLINK),
            ("t30", FEED_TWAP30),
            ("t60", FEED_TWAP60),
            ("bd", FEED_BINANCE_DIRECT),
            ("br", FEED_BINANCE_RELAY),
            ("bk", FEED_BOOK),
        )
    )
    reconnects = sum(state.reconnects.values())
    suffix = f"  rc={reconnects}" if reconnects else ""

    return (
        f"{now:>8}  "
        f"{_money(state.prices.get(FEED_CHAINLINK), 2):>11} "
        f"{_money(state.prices.get(FEED_TWAP30), 2):>11} "
        f"{_money(state.prices.get(FEED_TWAP60), 2):>11} "
        f"{_money(binance, 2):>11} "
        f"{_money(state.prices.get(FEED_BINANCE_RELAY), 2):>11}  "
        f"{basis_usd:>8} {basis.samples:>4}  "
        f"{bid:>6} {ask:>6}  {ages}{suffix}"
    )


def _money(value: Decimal | None, places: int) -> str:
    return "·" if value is None else f"{value:,.{places}f}"


def _age(state: FeedState, feed: str) -> str:
    """`--` = nunca llegó nada. `!` = por encima del umbral de staleness → halt (§5.2)."""
    age = state.age_s(feed)
    if age is None:
        return "--"
    return f"{age:.1f}{'!' if state.is_stale(feed) else ''}"


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
