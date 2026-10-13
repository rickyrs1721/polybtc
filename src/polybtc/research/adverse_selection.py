"""Fase 0.b · paso 2: ¿cuánto cuesta que te llenen? Selección adversa sobre los datos ya grabados.

El paso 1 dejó la economía del maker en dos piezas, y esta mide la única que falta:

    neto_por_fill = medio_spread_capturado + rebate_de_maker − selección_adversa

Las dos primeras son aritmética conocida (§2.2 y el paso 1 del plan). La tercera no se
sabe, y es la que decide: si te llenan justo cuando el precio se va en tu contra, pierdes
en cada operación antes de cualquier incentivo.

**Umbral escrito antes de medir** (`docs/market-making-plan.md`): si la selección adversa
a 60 s supera **medio tick (0,5¢)**, se para. Con el spread pegado al tick el 92,7% del
tiempo, medio tick es lo máximo que captura un maker en el toque por viaje de ida y vuelta.

## Cómo se estima un fill sin tener los trades

Solo se grabó el *top of book* a 1 Hz: no hay operaciones ni profundidad. Así que no se
mide, se estima, igual que pide el plan:

- Una compra en reposo en el bid a `p = bid(s−1)` se da por ejecutada en `s` si
  `ask(s) ≤ p`: el mejor ask bajó hasta nuestro nivel, luego alguien vendió contra él.
- Simétrico para una venta en reposo en el ask a `p = ask(s−1)`, ejecutada si `bid(s) ≥ p`.

Dos sesgos de este estimador, los dos en la misma dirección y hay que decirlos:

1. **Solo ve los fills que ocurren porque el precio se movió.** El flujo desinformado que
   cruza el spread sin mover el libro —te llenan y el bid se repone— es invisible aquí, y
   es justo el flujo del que vive un maker. El estimador mira el **peor subconjunto** de
   los fills.
2. Por construcción, el fill exige un movimiento de al menos un tick, así que el coste
   instantáneo (`k=0`) es mecánicamente ≈ un tick. Ese número no informa de nada. Lo que
   informa es si el mid **vuelve** en 10, 30 o 60 s.

Conclusión de método: este estimador está sesgado **hacia parar**. Un resultado bueno aquí
es creíble; uno malo dice «no se puede concluir que sea viable», no «es imposible». Lo que
lo arreglaría es grabar profundidad y operaciones — el paso 3 del plan.

## Las dos cifras y por qué el punto de referencia importa

El plan escribe el coste como `E[mid(t+k) − p]`. Medido contra el precio de ejecución, ese
número ya lleva dentro el medio spread que ganas por comprar por debajo del mid, así que
compararlo contra 0,5¢ contaría el medio spread dos veces. Se reportan los dos:

- `deriva` = `mid(s−1) − mid(s+k)` para una compra. Selección adversa pura: cuánto se mueve
  el mid en tu contra **desde antes del fill**. Es la que se compara con el umbral.
- `neto` = `mid(s+k) − p`. Resultado a mercado del fill, el del plan. Con el spread a un
  tick, `neto = medio_spread − deriva`, así que `deriva > 0,5¢` y `neto < 0` son el mismo
  test. Se imprimen juntos para que se vea.

Signo: **positivo = en tu contra** en `deriva`, positivo = a tu favor en `neto`.

Uso:
    uv run python -m polybtc.research.adverse_selection --data-dir ~/polybtc-backups/<host>
"""

from __future__ import annotations

import argparse
import glob
import statistics
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

TAU_BUCKETS: tuple[tuple[float, float], ...] = (
    (0.0, 30.0),
    (30.0, 60.0),
    (60.0, 120.0),
    (120.0, 300.0),
    (300.0, 600.0),
    (600.0, 900.0),
)

HORIZONS: tuple[int, ...] = (0, 10, 30, 60)
STOP_THRESHOLD_CENTS = 0.5
"""Umbral del paso 2, escrito antes de mirar los datos. Medio tick."""

CRYPTO_FEE_RATE = 0.07
MAKER_REBATE_RATE = 0.2


@dataclass(frozen=True, slots=True)
class Fill:
    """Un fill estimado, con todo lo necesario para juzgarlo. `None` = horizonte sin datos."""

    side: str
    tau_s: float
    tick: float
    price: float
    half_spread_cents: float
    drift_cents: tuple[float | None, ...]
    """Selección adversa por horizonte de `HORIZONS`. Positivo = el mid se fue en tu contra."""
    net_cents: tuple[float | None, ...]
    """`mid(s+k) − p` por horizonte. Positivo = el fill va a tu favor."""


def load(data_dir: Path) -> pa.Table:
    files = sorted(glob.glob(str(data_dir / "**" / "*.parquet"), recursive=True))
    if not files:
        raise SystemExit(f"no hay Parquet bajo {data_dir}")
    table = pa.concat_tables([pq.read_table(f) for f in files])
    return table.sort_by("ts")


def _sane(bid: float | None, ask: float | None) -> bool:
    """Un libro utilizable: los dos lados presentes, dentro de (0,1) y sin cruzar."""
    return bid is not None and ask is not None and 0.0 < bid < ask < 1.0


def scan(table: pa.Table, outcome: str) -> tuple[list[Fill], list[float]]:
    """Fills estimados en `outcome`, y el control.

    El control es la misma deriva medida en los segundos en los que **no** hubo fill. Sin
    él no se sabe si el coste lo crea el fill o una tendencia del propio mercado, igual
    que `mc_check.py` necesitaba la fórmula retirada para saber que el test tenía dientes.
    Se reporta el lado comprador; el vendedor es su negativo exacto.
    """
    ts: list[float | None] = [
        t.timestamp() if t is not None else None for t in table.column("ts").to_pylist()
    ]
    slug: list[str | None] = table.column("epoch_slug").to_pylist()
    tau: list[float | None] = table.column("tau_s").to_pylist()
    bid: list[float | None] = table.column(f"{outcome}_bid").to_pylist()
    ask: list[float | None] = table.column(f"{outcome}_ask").to_pylist()
    tick: list[float | None] = table.column(f"{outcome}_tick").to_pylist()

    def mid_at(index: int, origin: int) -> float | None:
        """Mid en `index`, exigiendo misma época, reloj contiguo y libro sano."""
        if not 0 <= index < len(ts):
            return None
        if slug[index] is None or slug[index] != slug[origin]:
            return None
        at, origin_at = ts[index], ts[origin]
        if at is None or origin_at is None:
            return None
        if round(at - origin_at) != index - origin:
            return None  # hueco en la grabación: el horizonte no es el que dice ser
        b, a = bid[index], ask[index]
        if b is None or a is None or not _sane(b, a):
            return None
        return (b + a) / 2.0

    fills: list[Fill] = []
    control: list[float] = []
    for i in range(1, len(ts)):
        prev = i - 1
        tau_i = tau[i]
        if slug[prev] is None or slug[prev] != slug[i] or tau_i is None:
            continue
        b0, a0, b1, a1 = bid[prev], ask[prev], bid[i], ask[i]
        if b0 is None or a0 is None or b1 is None or a1 is None:
            continue
        if not (_sane(b0, a0) and _sane(b1, a1)):
            continue
        mid0 = (b0 + a0) / 2.0
        half_spread = (a0 - b0) / 2.0

        sides = (("buy", b0, a1 <= b0), ("sell", a0, b1 >= a0))
        if not any(executed for _, _, executed in sides):
            quiet = mid_at(i + HORIZONS[-1], i)
            if quiet is not None:
                control.append((mid0 - quiet) * 100.0)

        for side, price, executed in sides:
            if not executed:
                continue
            drift: list[float | None] = []
            net: list[float | None] = []
            for k in HORIZONS:
                mid_k = mid_at(i + k, i)
                if mid_k is None:
                    drift.append(None)
                    net.append(None)
                    continue
                sign = 1.0 if side == "buy" else -1.0
                drift.append(sign * (mid0 - mid_k) * 100.0)
                net.append(sign * (mid_k - price) * 100.0)
            fills.append(
                Fill(
                    side=side,
                    tau_s=float(tau_i),
                    tick=float(tick[i] or 0.01),
                    price=price,
                    half_spread_cents=half_spread * 100.0,
                    drift_cents=tuple(drift),
                    net_cents=tuple(net),
                )
            )
    return fills, control


def _column(fills: Sequence[Fill], attr: str, horizon: int) -> list[float]:
    index = HORIZONS.index(horizon)
    values = [getattr(f, attr)[index] for f in fills]
    return [v for v in values if v is not None]


def _mean(values: Sequence[float]) -> float:
    return statistics.fmean(values) if values else float("nan")


def _p50(values: Sequence[float]) -> float:
    return statistics.median(values) if values else float("nan")


def report_rate(table: pa.Table, fills: Sequence[Fill], outcome: str) -> None:
    """Cuántos fills por hora y por época. Es el insumo del retorno del paso 1."""
    seconds = table.num_rows
    epochs = len({s for s in table.column("epoch_slug").to_pylist() if s})
    buys = sum(1 for f in fills if f.side == "buy")
    print(
        f"  libro {outcome}: {len(fills)} fills estimados "
        f"({buys} compra / {len(fills) - buys} venta) · "
        f"{len(fills) / (seconds / 3600.0):.1f}/h · {len(fills) / max(epochs, 1):.2f}/época"
    )


def report_overall(fills: Sequence[Fill]) -> None:
    print(f"  {'horizonte':>10}  {'n':>7}  {'deriva media':>13}  {'p50':>7}  {'neto medio':>11}")
    for k in HORIZONS:
        drift = _column(fills, "drift_cents", k)
        net = _column(fills, "net_cents", k)
        mark = "  <- umbral" if k == 60 and _mean(drift) > STOP_THRESHOLD_CENTS else ""
        print(
            f"  {k:>8} s  {len(drift):>7}  {_mean(drift):>12.3f}c  "
            f"{_p50(drift):>6.2f}c  {_mean(net):>10.3f}c{mark}"
        )


def rebate_cents(price: float) -> float:
    """Rebate de maker por share ejecutada, en céntimos.

    `fee = C x feeRate x p x (1-p)` y el maker cobra `rebate_rate` de esa cuantía, que es
    su cuota del pool cuando el pool se reparte con la misma fórmula que lo llena.
    Verificado el 2026-10-13 en docs.polymarket.com/programs/maker-rebates.md y contra el
    mercado vivo: `FeeSchedule(rate=0.07, taker_only=True, rebate_rate=0.2)`.
    """
    return MAKER_REBATE_RATE * CRYPTO_FEE_RATE * price * (1.0 - price) * 100.0


def report_by_tau(fills: Sequence[Fill], horizon: int) -> None:
    print(f"  deriva a {horizon} s por bucket de tau:")
    for lo, hi in TAU_BUCKETS:
        seg = [f for f in fills if lo < f.tau_s <= hi]
        drift = _column(seg, "drift_cents", horizon)
        net = _column(seg, "net_cents", horizon)
        if not drift:
            print(f"    {lo:>5.0f}-{hi:<5.0f}s  sin datos")
            continue
        verdict = "PARA" if _mean(drift) > STOP_THRESHOLD_CENTS else "pasa"
        print(
            f"    {lo:>5.0f}-{hi:<5.0f}s  n={len(drift):>6}  "
            f"deriva={_mean(drift):>7.3f}c  neto={_mean(net):>7.3f}c  {verdict}"
        )


def report_regimes(fills: Sequence[Fill]) -> None:
    """El tick no es constante (§8.1): medio céntimo significa cosas opuestas en cada régimen."""
    for tick in sorted({f.tick for f in fills}):
        seg = [f for f in fills if f.tick == tick]
        drift = _column(seg, "drift_cents", 60)
        half = _mean([f.half_spread_cents for f in seg])
        print(
            f"    tick {tick:<6}  n={len(seg):>7} ({len(seg) / len(fills):>5.1%})  "
            f"deriva60={_mean(drift):>7.3f}c  medio spread={half:>5.2f}c"
        )


def report_breakeven(fills: Sequence[Fill], horizon: int) -> None:
    """Convierte el sesgo del estimador en una condición comprobable.

    Este método solo ve los fills que ocurren porque el precio se movió. Un maker real
    también recibe flujo que cruza el spread sin mover el libro, y ese fill es benigno:
    capturas medio spread y cobras el rebate. Si una fracción `f` de tus fills son de los
    que sí mueven el libro, el neto esperado es

        f · (neto_adverso + rebate) + (1 − f) · (medio_spread + rebate)

    Lo que se imprime es la `f` que lo deja en cero: **por encima de esa fracción pierdes**.
    Es la pregunta que el paso 3 contestaría grabando profundidad y operaciones.
    """
    net = _column(fills, "net_cents", horizon)
    index = HORIZONS.index(horizon)
    rebate = _mean([rebate_cents(f.price) for f in fills if f.net_cents[index] is not None])
    half = _mean([f.half_spread_cents for f in fills])
    adverse = _mean(net) + rebate
    benign = half + rebate
    print(
        f"  por fill a {horizon} s: adverso={_mean(net):+.3f}c  rebate={rebate:+.3f}c  "
        f"-> adverso neto={adverse:+.3f}c   benigno={benign:+.3f}c"
    )
    if benign <= 0.0 or adverse >= 0.0:
        print("    sin punto de equilibrio: los dos tipos de fill van en el mismo sentido")
        return
    share = benign / (benign - adverse)
    print(
        f"    equilibrio: pierdes si más del {share:.1%} de tus fills son de los que "
        f"mueven el libro"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Fase 0.b paso 2: selección adversa")
    parser.add_argument("--data-dir", type=Path, required=True)
    args = parser.parse_args()

    table = load(args.data_dir)
    stamps = [s for s in table.column("ts").to_pylist() if s is not None]
    print(
        f"Fase 0.b · paso 2 · {table.num_rows} filas · "
        f"{stamps[0]:%d-%b %H:%M} a {stamps[-1]:%d-%b %H:%M} UTC"
    )
    print(f"Umbral escrito antes de medir: deriva a 60 s > {STOP_THRESHOLD_CENTS}c -> parar\n")

    for outcome in ("up", "down"):
        fills, control = scan(table, outcome)
        report_rate(table, fills, outcome)
        if not fills:
            continue
        print(
            f"  control (segundos sin fill, n={len(control)}): "
            f"deriva a {HORIZONS[-1]} s = {_mean(control):+.3f}c"
        )
        report_overall(fills)
        # A 60 s de horizonte los buckets de tau bajo quedan vacíos por construcción: el
        # horizonte no cabe dentro de la época y mirar al mercado siguiente no significa
        # nada. El de 10 s sí los cubre, y es justo donde la Fase 0 espera lo peor.
        report_by_tau(fills, 60)
        report_by_tau(fills, 10)
        report_regimes(fills)
        report_breakeven(fills, 60)
        print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
