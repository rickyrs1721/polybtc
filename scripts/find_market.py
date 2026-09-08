"""Descubre el mercado de la época vigente y escupe sus token ids. Solo lectura.

Existe porque **los token ids rotan con cada época** (§8.1) y por tanto no pueden vivir en
`config.py`. Entre el fin de una época y la creación de la siguiente hay una ventana de
~2 minutos en la que no existe ningún mercado abierto de la familia; `--wait` la espera.

Usa el SDK oficial `polymarket-client` (§1) contra la API Gamma, no el WebSocket: esto es
descubrimiento, no un feed. La lógica de aquí es el germen del descubrimiento que
necesitará el grabador de la Fase 0, que tampoco puede tener el mercado configurado.

Uso:
    uv run python scripts/find_market.py
    uv run python scripts/find_market.py --wait          # espera al cambio de época
    uv run python scripts/find_market.py --family btc-updown-5m
    export $(uv run python scripts/find_market.py --export)   # POLYBTC_TOKEN_IDS=...
"""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, datetime, timedelta

from polymarket import AsyncPublicClient, Market

from polybtc.config import Settings

POLL_INTERVAL_S = 15.0
"""Las épocas de 15 min no necesitan más resolución, y sondear más rápido no acelera nada."""

MAX_WAIT_S = 15 * 60.0
"""Techo duro: si en una época entera no aparece nada, el problema no es el hueco entre épocas."""


async def find_open_markets(family: str, *, horizon_h: int = 2) -> list[Market]:
    """Mercados de la familia abiertos y aceptando órdenes, el más cercano a vencer primero.

    `end_date_min=now` es lo que filtra las épocas ya cerradas. `accepting_orders` es un
    filtro aparte y necesario: un mercado puede seguir listado y no admitir órdenes.
    """
    now = datetime.now(UTC)
    async with AsyncPublicClient() as client:
        page = await client.list_markets(
            closed=False,
            end_date_min=now,
            end_date_max=now + timedelta(hours=horizon_h),
            order="endDate",
            ascending=True,
            page_size=300,
        ).first_page()
    return [m for m in page.items if family in (m.slug or "") and m.state.accepting_orders]


def describe(market: Market) -> str:
    trading = market.trading
    up, down = token_ids(market)
    return "\n".join(
        [
            f"  {market.question}",
            f"    slug          {market.slug}",
            f"    condition_id  {market.condition_id}",
            f"    tick={trading.minimum_tick_size}  min_order_size={trading.minimum_order_size}"
            f"  seconds_delay={trading.seconds_delay}",
            f"    bid/ask       {market.prices.best_bid} / {market.prices.best_ask}",
            f"    Up   (token0) {up}",
            f"    Down (token1) {down}",
        ]
    )


def token_ids(market: Market) -> tuple[str, str]:
    """`(Up, Down)`, en ese orden. El orden importa: es el que asume `POLYBTC_TOKEN_IDS`."""
    return str(market.outcomes.yes.token_id), str(market.outcomes.no.token_id)


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
    args = parser.parse_args()

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

    chosen = markets[0]
    ids = list(token_ids(chosen))

    if args.export:
        print("POLYBTC_TOKEN_IDS=" + json.dumps(ids, separators=(",", ":")))
        return 0

    print(f"{len(markets)} mercado(s) abiertos de {args.family!r}:\n")
    for market in markets:
        print(describe(market))
        print()
    print("Para el feed de book (§8.1: caduca al acabar la época):")
    print("  export POLYBTC_TOKEN_IDS='" + json.dumps(ids, separators=(",", ":")) + "'")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
