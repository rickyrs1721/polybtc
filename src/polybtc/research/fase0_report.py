"""Informe de la Fase 0: responde numéricamente las seis preguntas de §6 y el go/no-go.

Sin interpretación: solo números. La lectura está en `docs/fase0-informe.md`, y la
separación es deliberada — los números se vuelven a generar con este script cuando haga
falta, y la interpretación se discute aparte.

**El test que decide el proyecto es el Brier score**, no `|fair − mid|`. Esa distinción es
la trampa central de la Fase 0 y conviene decirla aquí: una discrepancia grande entre el
modelo y el mercado puede significar «hay oportunidad» o «mi modelo está mal», y
`|fair − mid|` **no distingue entre las dos**. La pregunta 2 de §6, leída sola, mide las
dos cosas sumadas.

Con los desenlaces reales grabados sí se distingue: se mira quién predice mejor lo que
acabó pasando. Si el mercado gana, la discrepancia era error propio. Y por si la respuesta
dependiera de la calibración de σ —que el soak ya señaló como sospechosa— el script barre
un factor multiplicativo sobre σ y se queda con el MEJOR caso posible para el modelo. Un
veredicto que solo aguanta con la σ que teníamos no valdría nada.

Uso:
    uv run python -m polybtc.research.fase0_report --data-dir /var/lib/polybtc/data
    uv run python -m polybtc.research.fase0_report --data-dir ~/polybtc-backups/<host>
"""

from __future__ import annotations

import argparse
import collections
import glob
import math
import sqlite3
import statistics
from collections.abc import Sequence
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from polybtc.model.binary import delta_per_dollar, fair_from_variance
from polybtc.model.twap import terminal_variance
from polybtc.model.vol import deannualize

TAU_BUCKETS: tuple[tuple[float, float], ...] = (
    (0.0, 30.0),
    (30.0, 60.0),
    (60.0, 120.0),
    (120.0, 300.0),
    (300.0, 600.0),
    (600.0, 900.0),
)

REFERENCE_SIGMA_ANNUAL = 0.60
REFERENCE_SPOT = 85_000.0
CUSHION_CENTS = 3.0
FEE_TAKER_CENTS = 1.75
BASIS_HALF_LIFE_S = 600.0
SIGMA_SCALES = (0.75, 1.0, 1.25, 1.5, 1.75, 2.0, 2.5, 3.0, 4.0)


def _pct(values: Sequence[float], q: float) -> float:
    return sorted(values)[max(0, int(len(values) * q) - 1)]


def _cents(dollars: float, tau_s: float, window_s: float = 60.0) -> float:
    """Dólares de error → céntimos de fair. La única unidad comparable con el colchón."""
    sigma_s = deannualize(REFERENCE_SIGMA_ANNUAL)
    variance = terminal_variance(sigma_s, tau_s, window_s)
    return dollars * delta_per_dollar(REFERENCE_SPOT, REFERENCE_SPOT, variance) * 100.0


def load(data_dir: Path) -> pa.Table:
    files = sorted(glob.glob(str(data_dir / "**" / "*.parquet"), recursive=True))
    if not files:
        raise SystemExit(f"no hay Parquet bajo {data_dir}")
    return pa.concat_tables([pq.read_table(f) for f in files])


def outcomes(data_dir: Path) -> dict[str, str]:
    """Desenlace por época. Es la variable dependiente de todo lo que decide algo aquí."""
    with sqlite3.connect(f"file:{data_dir / 'state.db'}?mode=ro", uri=True) as con:
        return dict(
            con.execute(
                "select slug, resolved_outcome from epochs where resolved_outcome in ('Up','Down')"
            ).fetchall()
        )


# -- Preguntas 1 a 6 de §6 ---------------------------------------------------------------


def q1_spread(t: pa.Table) -> None:
    bid, ask = t.column("up_bid").to_pylist(), t.column("up_ask").to_pylist()
    tick = t.column("up_tick").to_pylist()
    rows = [(a - b, k) for b, a, k in zip(bid, ask, tick, strict=True) if None not in (b, a)]
    spreads = [s for s, _ in rows]
    floor = [s <= (k or 0.01) * 1.5 for s, k in rows]
    print(
        f"P1  spread: n={len(spreads)}  p50={_pct(spreads, 0.5) * 100:.2f}c  "
        f"p90={_pct(spreads, 0.9) * 100:.2f}c  max={max(spreads) * 100:.1f}c"
    )
    print(f"    pegado al tick: {sum(floor) / len(floor):.1%} del tiempo")


def over(edges: Sequence[float], threshold_cents: float) -> float:
    return sum(1 for e in edges if e * 100 > threshold_cents) / len(edges)


def q2_q6_discrepancy(t: pa.Table) -> None:
    bid, ask, tau = (t.column(c).to_pylist() for c in ("up_bid", "up_ask", "tau_s"))
    for window in ("w60", "w900"):
        fair = t.column(f"fair_band_{window}").to_pylist()
        pairs = [
            (abs(f - (b + a) / 2), u)
            for f, b, a, u in zip(fair, bid, ask, tau, strict=True)
            if None not in (f, b, a, u)
        ]
        edges = [e for e, _ in pairs]
        print(
            f"P2  |fair-mid| {window}: media={statistics.fmean(edges) * 100:.2f}c  "
            f"p50={_pct(edges, 0.5) * 100:.2f}c  "
            f">{FEE_TAKER_CENTS}c={over(edges, FEE_TAKER_CENTS):.1%}  "
            f">{CUSHION_CENTS:.0f}c={over(edges, CUSHION_CENTS):.1%}"
        )
        parts = []
        for lo, hi in TAU_BUCKETS:
            seg = [e for e, u in pairs if lo < u <= hi]
            share = over(seg, CUSHION_CENTS) if seg else 0.0
            parts.append(f"{lo:.0f}-{hi:.0f}s {share:.0%}")
        print(f"P6    >{CUSHION_CENTS:.0f}c por tau: " + "  ".join(parts))


def q3_twap_lag(t: pa.Table) -> None:
    chainlink = t.column("chainlink").to_pylist()
    for column in ("twap30", "twap60"):
        twap = t.column(column).to_pylist()
        pairs = [(c, w) for c, w in zip(chainlink, twap, strict=True) if None not in (c, w)]
        spot = [p[0] for p in pairs]
        avg = [p[1] for p in pairs]

        def correlation(lag: int, spot: list[float] = spot, avg: list[float] = avg) -> float:
            a, b = spot[: len(spot) - lag], avg[lag:]
            ma, mb = statistics.fmean(a), statistics.fmean(b)
            num = sum((x - ma) * (y - mb) for x, y in zip(a, b, strict=True))
            den = math.sqrt(sum((x - ma) ** 2 for x in a) * sum((y - mb) ** 2 for y in b))
            return num / den if den else 0.0

        best = max(range(0, 90, 2), key=correlation)
        print(f"P3  {column}: equivale al spot retrasado ~{best} s (corr={correlation(best):.5f})")


def q4_book_survival(t: pa.Table) -> None:
    """Cuánto aguanta un nivel en el top of book antes de que el precio lo deje atrás."""
    for column in ("up_bid", "up_ask"):
        runs: list[int] = []
        length = 0
        previous: object = object()
        for value in t.column(column).to_pylist():
            if value is None:
                continue
            if value == previous:
                length += 1
            else:
                if length:
                    runs.append(length)
                length, previous = 1, value
        if length:
            runs.append(length)
        print(
            f"P4  {column}: el nivel aguanta p50={_pct(runs, 0.5):.0f}s  "
            f"p90={_pct(runs, 0.9):.0f}s  media={statistics.fmean(runs):.1f}s"
        )


def q5_basis(t: pa.Table) -> None:
    """El residuo DESPUÉS del EWMA, que es lo único que el modelo no puede corregir.

    Mirar la dispersión en crudo es el error fácil aquí: incluye la deriva lenta del nivel,
    que el estimador de §2.1.b sigue sin problema. Lo que envenena `ln(S/K)` es lo que queda
    después de restarla.
    """
    chainlink = t.column("chainlink").to_pylist()
    recorded = t.column("basis_usd").to_pylist()

    residual = {
        "binance": [
            (c - s) - b
            for c, s, b in zip(
                chainlink, t.column("binance_mid").to_pylist(), recorded, strict=True
            )
            if None not in (c, s, b)
        ]
    }
    # Coinbase no es la fuente configurada, así que su EWMA se reconstruye aquí con la
    # misma half-life para que la comparación sea de igual a igual.
    decay = 0.5 ** (1.0 / BASIS_HALF_LIFE_S)
    state: float | None = None
    coinbase: list[float] = []
    for c, s in zip(chainlink, t.column("coinbase_mid").to_pylist(), strict=True):
        if None in (c, s):
            continue
        observed = math.log(c / s)
        state = observed if state is None else decay * state + (1.0 - decay) * observed
        coinbase.append(s * (math.exp(observed) - math.exp(state)))
    residual["coinbase"] = coinbase

    for name, values in residual.items():
        deviation = statistics.pstdev(values)
        print(
            f"P5  residuo {name} tras EWMA: media=${statistics.fmean(values):+.2f}  "
            f"desv=${deviation:.2f}  p95=|${_pct([abs(v) for v in values], 0.95):.2f}|"
        )
        print(
            f"      -> {_cents(deviation, 300.0):.2f}c a tau=300s | "
            f"{_cents(deviation, 60.0):.2f}c a tau=60s  (colchon {CUSHION_CENTS:.0f}c)"
        )


# -- Lo que de verdad decide: contra los desenlaces ---------------------------------------


def settlement_test(t: pa.Table, resolved: dict[str, str], data_dir: Path) -> None:
    """§8: qué hipótesis de ventana predice los desenlaces. El empate resuelve Up (`>=`)."""
    slug, chainlink = t.column("epoch_slug").to_pylist(), t.column("chainlink").to_pylist()
    by_epoch: dict[str, list[float]] = collections.defaultdict(list)
    for s, c in zip(slug, chainlink, strict=True):
        if s and c is not None:
            by_epoch[s].append(c)
    mean_range = {s: statistics.fmean(v) for s, v in by_epoch.items()}

    with sqlite3.connect(f"file:{data_dir / 'state.db'}?mode=ro", uri=True) as con:
        rows = con.execute(
            "select slug, strike_chainlink, settle_twap30, settle_twap60, settle_chainlink "
            "from epochs where resolved_outcome in ('Up','Down') and strike_chainlink is not null"
        ).fetchall()

    score: collections.Counter[str] = collections.Counter()
    total = 0
    for s, strike, t30, t60, point in rows:
        candidates = {
            "w30 (twap30 en T)": t30,
            "w60 (twap60 en T)": t60,
            "puntual (chainlink en T)": point,
            "w900 (media del rango)": mean_range.get(s),
        }
        if any(v is None for v in candidates.values()) or strike is None:
            continue
        total += 1
        for name, value in candidates.items():
            if ("Up" if value >= strike else "Down") == resolved[s]:
                score[name] += 1

    print(f"\n§8  qué hipótesis predice los desenlaces ({total} épocas)")
    for name, hits in score.most_common():
        print(f"      {name:<26} {hits / total:>7.2%}   fallos: {total - hits}")


def brier_test(t: pa.Table, resolved: dict[str, str]) -> None:
    """¿Quién predice mejor: el modelo o el mercado? Brier score, menor es mejor.

    Es la única pregunta que separa «hay oportunidad» de «mi modelo está mal», y la que
    `|fair − mid|` no puede contestar.
    """
    columns = ("epoch_slug", "tau_s", "fair_band_w60", "fair_band_w900", "up_bid", "up_ask")
    slug, tau, w60, w900, bid, ask = (t.column(c).to_pylist() for c in columns)

    buckets: dict[float, dict[str, list[float]]] = collections.defaultdict(
        lambda: collections.defaultdict(list)
    )
    for s, u, a, b, p, q in zip(slug, tau, w60, w900, bid, ask, strict=True):
        if s not in resolved or None in (u, a, b, p, q) or not 0 < u <= 900:
            continue
        y = 1.0 if resolved[s] == "Up" else 0.0
        bucket = next(hi for lo, hi in TAU_BUCKETS if lo < u <= hi)
        buckets[bucket]["modelo w60"].append((a - y) ** 2)
        buckets[bucket]["modelo w900"].append((b - y) ** 2)
        buckets[bucket]["mercado"].append(((p + q) / 2 - y) ** 2)
        buckets[bucket]["trivial 0.50"].append((0.5 - y) ** 2)

    print(
        f"\nBRIER  (menor es mejor)\n{'tau<=':>6} {'n':>9} {'modelo w60':>12} "
        f"{'modelo w900':>12} {'mercado':>9} {'trivial':>8}   gana"
    )
    totals: dict[str, list[float]] = collections.defaultdict(list)
    for bucket in sorted(buckets):
        scores = {k: statistics.fmean(v) for k, v in buckets[bucket].items()}
        for k, v in buckets[bucket].items():
            totals[k] += v
        print(
            f"{bucket:>6.0f} {len(buckets[bucket]['mercado']):>9} "
            f"{scores['modelo w60']:>12.4f} {scores['modelo w900']:>12.4f} "
            f"{scores['mercado']:>9.4f} {scores['trivial 0.50']:>8.4f}   "
            f"{min(scores, key=lambda k: scores[k])}"
        )
    final = {k: statistics.fmean(v) for k, v in totals.items()}
    print(
        f"{'TOTAL':>6} {len(totals['mercado']):>9} {final['modelo w60']:>12.4f} "
        f"{final['modelo w900']:>12.4f} {final['mercado']:>9.4f} "
        f"{final['trivial 0.50']:>8.4f}   {min(final, key=lambda k: final[k])}"
    )


def sigma_sweep(t: pa.Table, resolved: dict[str, str]) -> None:
    """¿Y si σ estuviera bien calibrada? Se le da al modelo su MEJOR caso posible.

    Sin este barrido el veredicto sería discutible: «el modelo pierde, pero es que su σ
    estaba mal». Aquí se escala σ y se escoge el factor que más le favorece. Si aun así
    pierde, el problema no es la calibración.
    """
    columns = ("epoch_slug", "spot_eff_w60", "strike", "variance_w60", "up_bid", "up_ask")
    slug, spot_eff, strike, variance, bid, ask = (t.column(c).to_pylist() for c in columns)
    rows = [
        (se, k, v, (p + q) / 2, 1.0 if resolved[s] == "Up" else 0.0)
        for s, se, k, v, p, q in zip(slug, spot_eff, strike, variance, bid, ask, strict=True)
        if s in resolved and None not in (se, k, v, p, q) and v > 0
    ]
    market = statistics.fmean([(m - y) ** 2 for _, _, _, m, y in rows])

    print(f"\nBARRIDO DE σ  ({len(rows)} muestras) · mercado = {market:.4f}")
    best = (0.0, math.inf)
    for scale in SIGMA_SCALES:
        score = statistics.fmean(
            [(fair_from_variance(se, k, v * scale * scale) - y) ** 2 for se, k, v, _, y in rows]
        )
        best = min(best, (scale, score), key=lambda p: p[1])
        print(
            f"      σ x{scale:<5.2f} Brier={score:.4f}   "
            f"vs mercado {(market - score) / market:+.2%}"
        )
    print(
        f"      mejor: σ x{best[0]} -> {best[1]:.4f}, "
        f"{(best[1] - market) / market:+.2%} frente al mercado"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, required=True)
    args = parser.parse_args()

    table = load(args.data_dir)
    resolved = outcomes(args.data_dir)
    stamps = [s for s in table.column("ts").to_pylist() if s is not None]
    print(
        f"Fase 0 · {len(stamps)} filas · {stamps[0]:%d-%b %H:%M} a {stamps[-1]:%d-%b %H:%M} UTC "
        f"· {len(resolved)} épocas resueltas\n"
    )

    q1_spread(table)
    q2_q6_discrepancy(table)
    q3_twap_lag(table)
    q4_book_survival(table)
    q5_basis(table)
    settlement_test(table, resolved, args.data_dir)
    brier_test(table, resolved)
    sigma_sweep(table, resolved)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
