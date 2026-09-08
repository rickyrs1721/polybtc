"""Descubrimiento de la época por SLUG CONSTRUIDO, y lectura de su desenlace.

Los token ids rotan con cada época (§8.1) y por eso no pueden vivir en `config.py`: un
proceso que debe durar 14 días (§6, Fase 0) descubre el mercado, no lo lee de la config.

**El descubrimiento se hace construyendo el slug, no buscando por fecha de vencimiento.**
Medido contra la API el 2026-09-08 a las 21:48 UTC, y este es el motivo:

- El slug de estos mercados es `<familia>-<inicio de la época en segundos Unix>`. Verificado:
  `btc-updown-15m-1788903900` → 1788903900 = 21:45:00 UTC, y su `end_date` es 22:00:00.
  El sufijo es el INICIO, y el final es exactamente `inicio + duración`.
- Listar por `end_date` y quedarse con la primera página **no encuentra el mercado**. El
  servidor devuelve 100 elementos por página y hay ocho familias de cripto emitiendo un
  mercado cada 5 minutos: los `-5m` llenan la página entera y el `-15m`, que vence más
  tarde, queda fuera. Medido: a las 21:48 el mercado 21:45→22:00 existía y una búsqueda por
  `end_date` de una página devolvía cero.
- **No hay hueco entre épocas.** Las siguientes ocho épocas ya existían y aceptaban órdenes
  con más de dos horas de antelación. El «hueco de ~2 minutos» que este proyecto tenía
  anotado era este mismo fallo de paginación visto desde fuera.
- `state.start_date` **no es el inicio de la época**: es cuándo se creó el mercado, y llega
  con un día de antelación (`2026-09-07 22:01:23` para la época del 8 a las 21:45). Usarlo
  como instante de apertura pondría el strike a un día de distancia del que liquida.

Consecuencias prácticas: una sola petición por sondeo, la frontera de la época conocida
**exactamente** y no inferida, y la época siguiente disponible antes de que empiece — que
es lo que permite fijar el strike en el segundo de apertura y no diez segundos después.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from polymarket import AsyncPublicClient, Market

RESOLVED_TOLERANCE = Decimal("0.01")
"""Cuánto puede alejarse de 0 o 1 un precio ya resuelto antes de no creerle.

No se exige igualdad exacta: el precio viene de Gamma como cadena y el redondeo de la
plataforma no es un contrato. Lo que sí es señal es la separación — un mercado vivo cotiza
lejos de los extremos, y uno resuelto se pega a ellos.
"""

UMA_RESOLVED = "resolved"

OUTCOME_UP = "Up"
OUTCOME_DOWN = "Down"

PREFETCH_EPOCHS = 4
"""Cuántas épocas futuras se piden por sondeo.

Más de una a propósito: tener la siguiente ya en memoria es lo que permite adoptarla en el
segundo exacto en que empieza. Si hubiera que ir a la red en ese instante, el strike se
tomaría con el retraso del sondeo y §3.3 dejaría de tener el precio de apertura.
"""


@dataclass(frozen=True, slots=True)
class Epoch:
    """Una época de la familia objetivo: su mercado, su frontera exacta y sus dos tokens."""

    slug: str
    condition_id: str
    question: str
    start: datetime
    """Inicio, leído del propio slug. Es el instante contra el que se fija `K` (§8)."""

    end: datetime
    """Final, según `state.end_date`. Se comprueba que sea `start + duración`."""

    up_token: str
    down_token: str
    tick_size: Decimal | None
    min_order_size: Decimal | None
    seconds_delay: int | None
    created_at: datetime | None
    """`state.start_date`, que es cuándo se CREÓ el mercado (~un día antes), no su apertura."""

    @property
    def duration_s(self) -> float:
        return (self.end - self.start).total_seconds()

    def tau_s(self, now: datetime) -> float:
        """Segundos que faltan para el cierre. Nunca negativo (§3.0: todo en segundos)."""
        return max(0.0, (self.end - now).total_seconds())

    def contains(self, ts: datetime) -> bool:
        return self.start <= ts < self.end

    def tokens(self) -> tuple[str, str]:
        """`(Up, Down)`, en ese orden. El orden importa: es el de `POLYBTC_TOKEN_IDS`."""
        return self.up_token, self.down_token


def epoch_start_unix(now: datetime, epoch_seconds: float) -> int:
    """Inicio de la época que contiene `now`, alineado a la rejilla de la familia."""
    span = int(epoch_seconds)
    return (math.floor(now.timestamp()) // span) * span


def slug_for(family: str, start_unix: int) -> str:
    """`btc-updown-15m` + 1788903900 → `btc-updown-15m-1788903900`."""
    return f"{family}-{start_unix}"


def epoch_from_market(market: Market, *, epoch_seconds: float) -> Epoch | None:
    """`Market` de Gamma → `Epoch`, o `None` si no cuadra con la rejilla de la familia.

    El inicio sale del sufijo del slug y el final de `end_date`, y se exige que disten
    exactamente `epoch_seconds`. Si no coinciden, se descarta en vez de conciliarlos: son
    las dos únicas fuentes que hay sobre la frontera, y con ellas en desacuerdo cualquier
    `τ` o `K` que se calculara sería inventado.
    """
    end = market.state.end_date
    slug = market.slug
    condition_id = market.condition_id
    up = market.outcomes.yes.token_id
    down = market.outcomes.no.token_id
    if end is None or not slug or not condition_id or up is None or down is None:
        return None

    suffix = slug.rsplit("-", 1)[-1]
    if not suffix.isdigit():
        return None
    start = datetime.fromtimestamp(int(suffix), UTC)
    if abs((end - start).total_seconds() - epoch_seconds) > 1.0:
        return None

    return Epoch(
        slug=slug,
        condition_id=str(condition_id),
        question=market.question or "",
        start=start,
        end=end,
        up_token=str(up),
        down_token=str(down),
        tick_size=market.trading.minimum_tick_size,
        min_order_size=market.trading.minimum_order_size,
        seconds_delay=market.trading.seconds_delay,
        created_at=market.state.start_date,
    )


async def fetch_epochs(
    family: str, *, epoch_seconds: float, now: datetime, count: int = PREFETCH_EPOCHS
) -> list[Epoch]:
    """La época que contiene `now` y las `count-1` siguientes, ordenadas por inicio.

    Una sola petición filtrada por slug. Las épocas que la API no devuelva simplemente no
    salen en la lista: no se rellenan con nada, porque un mercado que no existe no tiene
    token ids que grabar.
    """
    span = int(epoch_seconds)
    first = epoch_start_unix(now, epoch_seconds)
    slugs = [slug_for(family, first + span * k) for k in range(count)]

    async with AsyncPublicClient() as client:
        page = await client.list_markets(slug=slugs, page_size=max(20, count * 2)).first_page()

    epochs = [
        epoch
        for market in page.items
        if (epoch := epoch_from_market(market, epoch_seconds=epoch_seconds)) is not None
    ]
    return sorted(epochs, key=lambda e: e.start)


async def find_open_markets(
    family: str, *, horizon_h: int = 2, max_items: int = 2000
) -> list[Market]:
    """Escaneo por fecha de vencimiento, paginado. Solo para la herramienta de consola.

    **Pagina de verdad**, a diferencia de la versión anterior: con una sola página el
    mercado de 15 minutos se queda fuera detrás de los de 5 minutos de ocho familias. El
    grabador no usa esto — usa `fetch_epochs`, que es determinista y cuesta una petición.
    """
    now = datetime.now(UTC)
    out: list[Market] = []
    async with AsyncPublicClient() as client:
        paginator = client.list_markets(
            closed=False,
            end_date_min=now,
            end_date_max=now + timedelta(hours=horizon_h),
            order="endDate",
            ascending=True,
            page_size=100,
        )
        async for market in paginator.iter_items():
            if family in (market.slug or "") and market.state.accepting_orders:
                out.append(market)
            max_items -= 1
            if max_items <= 0:
                break
    return out


async def fetch_market(slug: str) -> Market:
    async with AsyncPublicClient() as client:
        return await client.get_market(slug=slug)


def resolved_outcome(market: Market) -> str | None:
    """Desenlace de un mercado ya resuelto, o `None` si todavía no lo está.

    Este es el dato que decide la ventana TWAP de §8: con la frontera de la época, las dos
    series de TWAP grabadas a 1 s y el desenlace real, se puede ver cuál de las dos
    hipótesis (`w=60` del stream citado, `w=900` del rango del título) predice lo que
    efectivamente pasó. Sin él, 14 días de grabación no cierran la decisión.

    Se exige que la plataforma lo dé por resuelto **y** que los precios estén pegados a los
    extremos y sean complementarios. La primera condición sola no basta y la segunda tampoco:
    medido el 2026-09-08, una época recién vencida figuraba `closed=False` con `Up=0.005`
    —el libro ya sabía el desenlace pero la resolución no estaba publicada—, y leer ese
    precio como si fuera el desenlace contaminaría la única variable dependiente que tiene
    este experimento.
    """
    settled = bool(market.state.closed) or (
        (market.resolution.uma_resolution_status or "") == UMA_RESOLVED
    )
    if not settled:
        return None
    up = market.outcomes.yes.price
    down = market.outcomes.no.price
    if up is None or down is None:
        return None
    if abs(up + down - Decimal(1)) > RESOLVED_TOLERANCE:
        return None
    if abs(up - Decimal(1)) <= RESOLVED_TOLERANCE:
        return OUTCOME_UP
    if abs(up) <= RESOLVED_TOLERANCE:
        return OUTCOME_DOWN
    return None


def current_of(epochs: Sequence[Epoch], now: datetime) -> Epoch | None:
    """La época de la lista que contiene `now`, si alguna."""
    for epoch in epochs:
        if epoch.contains(now):
            return epoch
    return None
