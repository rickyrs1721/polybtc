"""Lectura de los Parquet recientes y agregados de los paneles. Solo lectura, siempre.

Los paneles no se eligen por lo que quede bonito: cada uno responde a una pregunta de §6.

| Panel                                   | Pregunta                    |
| --------------------------------------- | --------------------------- |
| Serie de basis                          | Fase 0 · 5                  |
| σ y su banda                            | §3.4                        |
| fair vs mid vs bid/ask                  | Fase 0 · 2                  |
| Histograma de \\|fair − mid\\| con 1.75¢ y 3¢ | Fase 0 · 2             |
| \\|fair − mid\\| por bucket de τ          | Fase 0 · 6                  |

Todo se calcula en Python plano sobre columnas de Arrow, sin pandas. Un día entero a 1 Hz
son 86.400 filas y unas pocas columnas: no hace falta más maquinaria, y así el dashboard no
arrastra una dependencia que el grabador no necesita.

**Se lee también el `.arrow.part` de la hora en curso.** Un panel que solo mirase los
Parquet cerrados iría hasta una hora por detrás, y el sentido de mirar el dashboard una vez
al día (§6, Fase 0) es ver lo que está pasando, no lo que pasó antes de la hora en punto.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from polybtc.recorder.sink import PARQUET_SUFFIX, PART_SUFFIX, read_part

FEE_TAKER_AT_HALF = 0.0175
"""Fee taker a p=0.50: `0.07·p·(1−p)` (§2.2). Primera línea del histograma."""

MODEL_CUSHION = 0.03
"""Colchón de modelo de §3.5. Segunda línea: por debajo, no hay decisión que tomar."""

TAU_BUCKETS: tuple[tuple[float, float], ...] = (
    (0.0, 10.0),
    (10.0, 30.0),
    (30.0, 60.0),
    (60.0, 120.0),
    (120.0, 300.0),
    (300.0, 600.0),
    (600.0, 900.0),
)
"""Buckets de τ de la pregunta 6 de §6.

El corte de 30 s no es arbitrario: es la frontera que la tabla de decisión del go/no-go
usa para separar «edge de modelo» de «edge de latencia». Si el edge solo aparece por debajo
de ese corte, no es nuestro (§2.3).
"""

MAX_SERIES_POINTS = 720
"""Puntos por serie tras submuestrear. Más no se ve en pantalla y sí se nota al cargar."""


@dataclass(frozen=True, slots=True)
class Window:
    """Una hipótesis de ventana TWAP, con el sufijo con el que vive en el Parquet."""

    suffix: str

    @property
    def fair_column(self) -> str:
        return f"fair_band_{self.suffix}"

    @property
    def label(self) -> str:
        return self.suffix


def recent_files(root: Path, hours: int, now: datetime) -> list[Path]:
    """Archivos que cubren las últimas `hours` horas, en orden cronológico.

    La selección es por la RUTA (`YYYY/MM/DD/HH`), no por el contenido: abrir 336 archivos
    para mirar su primera fila costaría más que el propio panel.
    """
    if not root.exists():
        return []
    wanted = {
        (now - timedelta(hours=offset)).strftime("%Y/%m/%d/%H") for offset in range(hours + 1)
    }
    found: list[tuple[str, Path]] = []
    for path in root.rglob("*"):
        if path.suffix not in {PARQUET_SUFFIX, ".part"}:
            continue
        stem = _hour_key_of(path)
        if stem is not None and stem in wanted:
            found.append((stem + path.name, path))
    return [path for _, path in sorted(found)]


def load(root: Path, hours: int, columns: Sequence[str], now: datetime) -> dict[str, list[Any]]:
    """Columnas pedidas de las últimas `hours` horas, concatenadas y ordenadas por `ts`.

    Una columna que no exista en un archivo se rellena con `None` en vez de reventar: si la
    configuración de hipótesis de ventana cambia (§8), los archivos viejos siguen siendo
    legibles y el panel simplemente no tiene ese dato para ese tramo.
    """
    needed = list(dict.fromkeys(["ts", *columns]))
    out: dict[str, list[Any]] = {name: [] for name in needed}

    for path in recent_files(root, hours, now):
        table = _read(path)
        if table is None or table.num_rows == 0:
            continue
        available = set(table.schema.names)
        for name in needed:
            if name in available:
                out[name].extend(table.column(name).to_pylist())
            else:
                out[name].extend([None] * table.num_rows)

    # Una fila sin `ts` no debería existir, pero si existiera va al principio en vez de
    # reventar el orden: el panel se dibuja con lo que hay, no se cae por una fila rara.
    epoch_zero = datetime.min.replace(tzinfo=UTC)
    order = sorted(range(len(out["ts"])), key=lambda i: out["ts"][i] or epoch_zero)
    return {name: [values[i] for i in order] for name, values in out.items()}


def series_panel(
    root: Path, hours: int, windows: Sequence[Window], now: datetime
) -> dict[str, Any]:
    """Series de basis, σ y fair-vs-mid. Preguntas 5 y 2 de §6, y §3.4."""
    fair_columns = [w.fair_column for w in windows]
    data = load(
        root,
        hours,
        ["basis_usd", "sigma_annual", "up_bid", "up_ask", "tau_s", *fair_columns],
        now,
    )
    stamps = [_iso(ts) for ts in data["ts"]]
    mids = [_mid(b, a) for b, a in zip(data["up_bid"], data["up_ask"], strict=True)]

    panel: dict[str, Any] = {
        "hours": hours,
        "points": len(stamps),
        "ts": _thin(stamps),
        "basis_usd": _thin(data["basis_usd"]),
        "sigma_annual": _thin(data["sigma_annual"]),
        "mid": _thin(mids),
        "bid": _thin(data["up_bid"]),
        "ask": _thin(data["up_ask"]),
        "fair": {w.label: _thin(data[w.fair_column]) for w in windows},
    }
    panel["basis_stats"] = _describe([v for v in data["basis_usd"] if v is not None])
    return panel


def edge_panel(root: Path, hours: int, windows: Sequence[Window], now: datetime) -> dict[str, Any]:
    """Histograma de |fair − mid| y su desglose por bucket de τ. Preguntas 2 y 6 de §6.

    Se calcula para CADA hipótesis de ventana porque §8 sigue abierta: presentar una sola
    sería elegir la respuesta antes de tener el dato que la decide.
    """
    fair_columns = [w.fair_column for w in windows]
    data = load(root, hours, ["up_bid", "up_ask", "tau_s", *fair_columns], now)
    mids = [_mid(b, a) for b, a in zip(data["up_bid"], data["up_ask"], strict=True)]

    result: dict[str, Any] = {
        "hours": hours,
        "fee_taker": FEE_TAKER_AT_HALF,
        "cushion": MODEL_CUSHION,
        "windows": {},
    }
    for window in windows:
        fairs = data[window.fair_column]
        pairs = [
            (abs(fair - mid), tau)
            for fair, mid, tau in zip(fairs, mids, data["tau_s"], strict=True)
            if fair is not None and mid is not None and tau is not None
        ]
        edges = [edge for edge, _ in pairs]
        result["windows"][window.label] = {
            "samples": len(edges),
            "histogram": _histogram(edges),
            "above_fee": _fraction(edges, FEE_TAKER_AT_HALF),
            "above_cushion": _fraction(edges, MODEL_CUSHION),
            "stats": _describe(edges),
            "by_tau": _by_tau(pairs),
        }
    return result


SPREAD_RESOLUTION = 0.001
"""Resolución del histograma de spread, en unidades de precio.

Fija en 0,001 —el tick más fino que usa la plataforma— y NO igual al tick vigente, que es
lo que parecía natural. El motivo: el tick sigue al precio (0,01 cerca del medio, 0,001
cerca de los extremos), así que un histograma en «múltiplos del tick» mezclaría dos escalas
distintas en las mismas barras y un 1 significaría 1¢ en unas filas y 0,1¢ en otras.
"""

SPREAD_MAX_BUCKETS = 20
"""Hasta 2¢ con detalle; por encima, un bucket de desbordamiento."""


def book_panel(root: Path, hours: int, now: datetime, *, tick: float) -> dict[str, Any]:
    """Distribución del spread y salud del libro. Pregunta 1 de §6 — y coartada de la 6.

    La pregunta 1 («¿cuál es la distribución del spread?») parece la menos interesante de
    las seis y es la que sostiene a las otras dos:

    **Es el techo de lo que se puede capturar.** Un maker se pone DENTRO del spread, así
    que lo que aspira a ganar está acotado por su mitad. Leer «|fair − mid| supera 3¢ el X%
    del tiempo» sin mirar el spread al lado es leer la pregunta 2 de más.

    **Y es lo único que distingue dos lecturas opuestas de la pregunta 6.** Si `|fair−mid|`
    crece cuando `τ` baja, eso puede ser edge de modelo o puede ser que el libro se vacíe y
    el `mid` deje de significar nada. Son indistinguibles en los datos salvo que se mire el
    spread por el MISMO bucket de τ. Por eso `by_tau` usa aquí los mismos cortes que
    `edge_panel`: las dos tablas están hechas para leerse fila a fila.

    **«Pegado al suelo» se juzga contra el tick GRABADO en cada fila, no contra uno fijo.**
    El tick sigue al precio: medido el 2026-09-11, una época deep-OTM cotizaba con `0.001` y
    tres épocas a la vez en torno a `0.51` con `0.01`. Un spread de medio céntimo es un libro
    pegado al suelo en régimen fino y un libro imposible en régimen grueso — la misma cifra,
    dos lecturas opuestas. `tick` solo es el respaldo para las filas que no lo tengan.
    """
    data = load(root, hours, ["up_bid", "up_ask", "down_bid", "down_ask", "up_tick", "tau_s"], now)
    spreads: list[float] = []
    pairs: list[tuple[float, float]] = []
    at_floor_flags: list[bool] = []
    floor_by_tau: list[tuple[bool, float]] = []
    complementarity: list[float] = []
    ticks_seen: set[float] = set()

    for up_bid, up_ask, down_bid, down_ask, up_tick, tau in zip(
        data["up_bid"],
        data["up_ask"],
        data["down_bid"],
        data["down_ask"],
        data["up_tick"],
        data["tau_s"],
        strict=True,
    ):
        if up_bid is not None and up_ask is not None:
            spread = up_ask - up_bid
            row_tick = up_tick if up_tick else tick
            ticks_seen.add(row_tick)
            # Medio tick de margen para no castigar el redondeo del float; un libro al suelo
            # vale exactamente un tick y el siguiente escalón está a dos.
            at_floor = spread <= row_tick * 1.5
            spreads.append(spread)
            at_floor_flags.append(at_floor)
            if tau is not None:
                pairs.append((spread, tau))
                floor_by_tau.append((at_floor, tau))
        up_mid = _mid(up_bid, up_ask)
        down_mid = _mid(down_bid, down_ask)
        if up_mid is not None and down_mid is not None:
            # Los dos outcomes de una binaria tienen que sumar 1. Cuando no lo hacen, o hay
            # una oportunidad real o —mucho más probable— algo va mal en el feed del libro.
            # Es un canario de calidad de datos, y en 14 días se quiere ver el día 1.
            complementarity.append(up_mid + down_mid - 1.0)

    return {
        "hours": hours,
        "tick": tick,
        "ticks_seen": sorted(ticks_seen),
        "samples": len(spreads),
        "stats": _describe(spreads),
        "at_floor": _share(at_floor_flags),
        "resolution": SPREAD_RESOLUTION,
        "by_tick": _spread_histogram(spreads),
        "by_tau": _spread_by_tau(pairs, floor_by_tau),
        "complementarity": _describe(complementarity),
    }


# -- interno ---------------------------------------------------------------------------


def _read(path: Path) -> pa.Table | None:
    try:
        if path.name.endswith(PART_SUFFIX):
            batches = list(read_part(path))
            return pa.Table.from_batches(batches) if batches else None
        return pq.read_table(path)
    except (pa.ArrowInvalid, OSError):
        return None  # un archivo ilegible no puede tumbar el dashboard


def _hour_key_of(path: Path) -> str | None:
    """`data/2026/09/08/17.parquet` → `2026/09/08/17`, o `None` si no encaja."""
    parts = path.parts
    if len(parts) < 4:
        return None
    year, month, day = parts[-4], parts[-3], parts[-2]
    hour = path.name.split(".")[0]
    if not (year.isdigit() and month.isdigit() and day.isdigit() and hour.isdigit()):
        return None
    return f"{year}/{month}/{day}/{hour}"


def _mid(bid: float | None, ask: float | None) -> float | None:
    if bid is None or ask is None:
        return None
    return (bid + ask) / 2.0


def _iso(ts: Any) -> str | None:
    if isinstance(ts, datetime):
        return ts.astimezone(UTC).isoformat()
    return None


def _thin(values: Sequence[Any]) -> list[Any]:
    """Submuestrea a `MAX_SERIES_POINTS` tomando uno de cada n, sin promediar.

    Sin promediar a propósito: una media suavizaría justo los picos de basis y de edad que
    el panel existe para hacer visibles.
    """
    if len(values) <= MAX_SERIES_POINTS:
        return list(values)
    step = math.ceil(len(values) / MAX_SERIES_POINTS)
    return list(values[::step])


def _describe(values: Sequence[float]) -> dict[str, float | None]:
    if not values:
        return {"n": 0, "mean": None, "std": None, "min": None, "max": None, "p50": None}
    ordered = sorted(values)
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    return {
        "n": len(values),
        "mean": mean,
        "std": math.sqrt(variance),
        "min": ordered[0],
        "max": ordered[-1],
        "p50": ordered[len(ordered) // 2],
    }


def _fraction(values: Sequence[float], threshold: float) -> float | None:
    if not values:
        return None
    return sum(1 for v in values if v > threshold) / len(values)


def _histogram(values: Sequence[float], bins: int = 40, top: float = 0.20) -> dict[str, Any]:
    """Histograma de |fair − mid| en [0, `top`], con desbordamiento en el último bin."""
    counts = [0] * bins
    width = top / bins
    for value in values:
        index = min(bins - 1, int(value / width)) if value > 0 else 0
        counts[index] += 1
    return {
        "width": width,
        "edges": [i * width for i in range(bins + 1)],
        "counts": counts,
    }


def _share(flags: Sequence[bool]) -> float | None:
    if not flags:
        return None
    return sum(1 for flag in flags if flag) / len(flags)


def _spread_histogram(spreads: Sequence[float]) -> list[dict[str, Any]]:
    """Cuenta de spreads en escalones de `SPREAD_RESOLUTION`, con desbordamiento al final.

    El desbordamiento no se descarta: un libro que a veces se abre a 20¢ es información
    sobre cuándo NO se puede cotizar, y esconderlo en el eje daría una distribución más
    bonita y menos cierta.
    """
    counts = [0] * (SPREAD_MAX_BUCKETS + 1)
    for spread in spreads:
        index = min(SPREAD_MAX_BUCKETS, max(0, round(spread / SPREAD_RESOLUTION)))
        counts[index] += 1
    total = len(spreads) or 1
    return [
        {
            "steps": i,
            "spread": i * SPREAD_RESOLUTION,
            "overflow": i == SPREAD_MAX_BUCKETS,
            "count": count,
            "share": count / total,
        }
        for i, count in enumerate(counts)
    ]


def _spread_by_tau(
    pairs: Sequence[tuple[float, float]], floors: Sequence[tuple[bool, float]]
) -> list[dict[str, Any]]:
    """Spread medio por bucket de τ. Mismos cortes que `_by_tau`: van juntas o no sirven."""
    rows: list[dict[str, Any]] = []
    for low, high in TAU_BUCKETS:
        values = [spread for spread, tau in pairs if low < tau <= high]
        flags = [flag for flag, tau in floors if low < tau <= high]
        rows.append(
            {
                "low": low,
                "high": high,
                "samples": len(values),
                "mean": None if not values else sum(values) / len(values),
                "at_floor": _share(flags),
            }
        )
    return rows


def _by_tau(pairs: Sequence[tuple[float, float]]) -> list[dict[str, Any]]:
    """|fair − mid| por bucket de τ: la pregunta 6 de §6, que decide si el edge es nuestro."""
    rows: list[dict[str, Any]] = []
    for low, high in TAU_BUCKETS:
        edges = [edge for edge, tau in pairs if low < tau <= high]
        rows.append(
            {
                "low": low,
                "high": high,
                "samples": len(edges),
                "mean": None if not edges else sum(edges) / len(edges),
                "above_fee": _fraction(edges, FEE_TAKER_AT_HALF),
                "above_cushion": _fraction(edges, MODEL_CUSHION),
            }
        )
    return rows
