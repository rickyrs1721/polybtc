"""Descubre el mercado de la época vigente y escupe sus token ids. Solo lectura.

Herramienta de consola sobre `feeds/discovery.py`, que es donde vive la lógica de verdad
porque el grabador de la Fase 0 la necesita igual: **los token ids rotan con cada época**
(§8.1) y por eso no pueden vivir en `config.py`. Entre el fin de una época y la creación de
la siguiente hay una ventana de ~2 minutos sin ningún mercado abierto; `--wait` la espera.

Uso:
    uv run python scripts/find_market.py
    uv run python scripts/find_market.py --wait          # espera al cambio de época
    uv run python scripts/find_market.py --family btc-updown-5m
    uv run python scripts/find_market.py --resolved btc-updown-15m-...  # desenlace
    export $(uv run python scripts/find_market.py --export)   # POLYBTC_TOKEN_IDS=...
"""

from __future__ import annotations

import argparse
import asyncio
import json

from polymarket import Market

from polybtc.config import Settings
from polybtc.feeds.discovery import (
    epoch_from_market,
    fetch_market,
    find_open_markets,
    resolved_outcome,
)

POLL_INTERVAL_S = 15.0
"""Las épocas de 15 min no necesitan más resolución, y sondear más rápido no acelera nada."""

MAX_WAIT_S = 15 * 60.0
"""Techo duro: si en una época entera no aparece nada, el problema no es el hueco entre épocas."""


def describe(market: Market, *, epoch_seconds: float) -> str:
    trading = market.trading
    epoch = epoch_from_market(market, epoch_seconds=epoch_seconds)
    lines = [
        f"  {market.question}",
        f"    slug          {market.slug}",
        f"    condition_id  {market.condition_id}",
        f"    tick={trading.minimum_tick_size}  min_order_size={trading.minimum_order_size}"
        f"  seconds_delay={trading.seconds_delay}",
        f"    bid/ask       {market.prices.best_bid} / {market.prices.best_ask}",
    ]
    if epoch is not None:
        # El inicio sale del SUFIJO DEL SLUG. `state.start_date` se muestra al lado para que
        # se vea por qué no se usa: es cuándo se creó el mercado, con ~un día de antelación.
        lines += [
            f"    rango         {epoch.start.isoformat()} → {epoch.end.isoformat()}",
            f"    creado        {epoch.created_at} (state.start_date, NO la apertura)",
            f"    Up   (token0) {epoch.up_token}",
            f"    Down (token1) {epoch.down_token}",
        ]
    return "\n".join(lines)


async def show_resolution(slug: str) -> int:
    market = await fetch_market(slug)
    outcome = resolved_outcome(market)
    print(f"{slug}: {outcome or 'sin resolver todavía'}")
    print(f"  closed={market.state.closed}  Up={market.outcomes.yes.price}")
    return 0 if outcome else 1


async def main() -> int:
    settings = Settings()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--family", default=settings.market_slug_prefix)
    parser.add_argument(
        "--wait", action="store_true", help="espera a que se cree la época siguiente"
    )
    parser.add_argument(
        "--export", action="store_true", help="imprime solo POLYBTC_TOKEN_IDS=[...]"
    )
    parser.add_argument("--resolved", metavar="SLUG", help="lee el desenlace de una época")
    args = parser.parse_args()

    if args.resolved:
        return await show_resolution(args.resolved)

    waited = 0.0
    while True:
        markets = await find_open_markets(args.family)
        if markets:
            break
        if not args.wait:
            print(f"no hay ningún mercado abierto de la familia {args.family!r}.")
            print("Puede ser el hueco de ~2 min entre épocas (§8.1): reintenta con --wait.")
            return 1
        if waited >= MAX_WAIT_S:
            print(f"nada en {MAX_WAIT_S / 60:.0f} min: no es el hueco entre épocas.")
            return 1
        await asyncio.sleep(POLL_INTERVAL_S)
        waited += POLL_INTERVAL_S

    chosen = epoch_from_market(markets[0], epoch_seconds=settings.epoch_seconds)
    ids = [] if chosen is None else list(chosen.tokens())

    if args.export:
        print("POLYBTC_TOKEN_IDS=" + json.dumps(ids, separators=(",", ":")))
        return 0

    print(f"{len(markets)} mercado(s) abiertos de {args.family!r}:\n")
    for market in markets:
        print(describe(market, epoch_seconds=settings.epoch_seconds))
        print()
    print("Para el feed de book (§8.1: caduca al acabar la época):")
    print("  export POLYBTC_TOKEN_IDS='" + json.dumps(ids, separators=(",", ":")) + "'")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
