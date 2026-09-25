"""Veredicto del soak: ¿puede empezar el contador de 14 días, o todavía no?

Existe porque hay una diferencia entre «el código funciona» y «esto aguanta un día
entero sin que nadie lo mire», y la Fase 0 solo vale si es lo segundo. Cuando se escribió,
el grabador nunca había corrido más de 22 minutos seguidos ni había cruzado en vivo un
cambio de hora — que es justo cuando cierra el Parquet y hace el `rename` atómico.

Los umbrales están **escritos antes** de mirar ningún resultado, igual que la tabla de
go/no-go de §6. Decidirlos después de ver los números es cómo se justifica cualquier cosa.

Uso, sobre el VPS o sobre una copia ya traída:
    uv run python -m deploy.soak_report --data-dir /var/lib/polybtc/data
    uv run python -m deploy.soak_report --data-dir ~/polybtc-backups/mi-vps --hours 48
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from polybtc.model.binary import delta_per_dollar
from polybtc.model.twap import terminal_variance
from polybtc.model.vol import deannualize
from polybtc.recorder.sink import PART_SUFFIX, read_part

# -- Umbrales, escritos antes de correr nada -------------------------------------------

MIN_HOURS = 24.0
"""Menos de un día no prueba nada: no habrá cruzado suficientes cambios de hora ni épocas."""

MAX_GAP_S = 2.5
"""Un hueco mayor que esto entre filas consecutivas es un segundo perdido, no jitter."""

MAX_GAP_SHARE = 0.001
"""Como mucho un 0,1% de los segundos con hueco. Por encima, la grabación tiene agujeros."""

MIN_EPOCHS = 8
"""Ocho épocas de 15 min son dos horas: suficiente para ver el relevo funcionando varias veces."""

MIN_STRIKE_OPEN_SHARE = 0.90
"""Casi todas las épocas deben fijar el strike en la apertura.

Una `missed` suelta por reinicio es normal; varias significan que el relevo no funciona."""

MIN_RESOLVED_SHARE = 0.80
"""Sin desenlace no hay experimento: es la variable dependiente de la ventana TWAP (§8)."""

MAX_RECONNECTS_PER_HOUR = 6.0

MIN_FAIR_SHARE = 0.50
"""Al menos la mitad de los segundos deben producir fair. Por debajo, algo cegó el modelo."""


class Verdict(StrEnum):
    PASS = "OK"
    WARN = "AVISO"
    FAIL = "FALLA"

    @property
    def rank(self) -> int:
        return {Verdict.PASS: 0, Verdict.WARN: 1, Verdict.FAIL: 2}[self]


@dataclass(frozen=True, slots=True)
class Check:
    name: str
    verdict: Verdict
    detail: str

    def render(self) -> str:
        mark = {Verdict.PASS: "·", Verdict.WARN: "!", Verdict.FAIL: "X"}[self.verdict]
        return f" {mark} {self.verdict.value:<6} {self.name:<32} {self.detail}"


def _describe(values: Sequence[float]) -> dict[str, float]:
    """Media, desviación y extremos. Local a propósito: esta herramienta corre sola."""
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    return {
        "n": float(len(values)),
        "mean": mean,
        "std": math.sqrt(variance),
        "min": min(values),
        "max": max(values),
    }


def _verdict(value: float, warn: float, fail: float, *, higher_is_better: bool) -> Verdict:
    if higher_is_better:
        if value < fail:
            return Verdict.FAIL
        return Verdict.WARN if value < warn else Verdict.PASS
    if value > fail:
        return Verdict.FAIL
    return Verdict.WARN if value > warn else Verdict.PASS


# -- Carga -------------------------------------------------------------------------------


def load_table(data_dir: Path, hours: float) -> pa.Table | None:
    """Todo lo grabado en las últimas `hours` horas, Parquet cerrados y hora en curso."""
    cutoff = datetime.now(UTC) - timedelta(hours=hours)
    tables: list[pa.Table] = []

    for path in sorted(data_dir.rglob("*.parquet")):
        try:
            tables.append(pq.read_table(path))
        except (pa.ArrowInvalid, OSError):
            print(f"   AVISO: no se pudo leer {path}")
    for path in sorted(data_dir.rglob(f"*{PART_SUFFIX}")):
        batches = list(read_part(path))
        if batches:
            tables.append(pa.Table.from_batches(batches))

    if not tables:
        return None
    table = pa.concat_tables(tables)
    stamps = table.column("ts").to_pylist()
    keep = [i for i, ts in enumerate(stamps) if ts is not None and ts >= cutoff]
    return table.take(keep) if keep else None


# -- Comprobaciones ----------------------------------------------------------------------


def check_coverage(table: pa.Table, data_dir: Path) -> list[Check]:
    stamps = sorted(ts for ts in table.column("ts").to_pylist() if ts is not None)
    span_h = (stamps[-1] - stamps[0]).total_seconds() / 3600.0
    checks = [
        Check(
            "Duración",
            _verdict(span_h, MIN_HOURS, MIN_HOURS * 0.5, higher_is_better=True),
            f"{span_h:.1f} h ({len(stamps)} filas), "
            f"de {stamps[0]:%d-%b %H:%M} a {stamps[-1]:%H:%M} UTC",
        )
    ]

    gaps = [
        (stamps[i - 1], (stamps[i] - stamps[i - 1]).total_seconds())
        for i in range(1, len(stamps))
        if (stamps[i] - stamps[i - 1]).total_seconds() > MAX_GAP_S
    ]
    share = len(gaps) / max(len(stamps), 1)
    lost = sum(seconds for _, seconds in gaps)
    checks.append(
        Check(
            "Continuidad del segundero",
            _verdict(share, MAX_GAP_SHARE, MAX_GAP_SHARE * 10, higher_is_better=False),
            f"{len(gaps)} huecos > {MAX_GAP_S:.1f} s ({share * 100:.3f}%), {lost:.0f} s perdidos"
            + (f". El mayor a las {max(gaps, key=lambda g: g[1])[0]:%H:%M:%S}" if gaps else ""),
        )
    )

    # Que haya varios Parquet cerrados es la prueba de que el cambio de hora funcionó en
    # vivo: cada uno es un `rename` atómico que ocurrió sin nadie delante.
    closed = list(data_dir.rglob("*.parquet"))
    pending = list(data_dir.rglob(f"*{PART_SUFFIX}"))
    checks.append(
        Check(
            "Cambios de hora",
            _verdict(len(closed), 12, 2, higher_is_better=True),
            f"{len(closed)} Parquet cerrados y legibles, {len(pending)} hora(s) en curso",
        )
    )

    # Se extrapola SOLO desde los Parquet ya cerrados. El `.part` de la hora en curso es
    # Arrow IPC sin comprimir, así que mezclarlo infla la cifra hasta el triple: medido,
    # 45 MB/día contando el `.part` frente a los ~16 MB/día reales del Parquet con zstd.
    # Una previsión de disco inflada en la primera hora manda a redimensionar una máquina
    # que no lo necesita.
    if closed:
        closed_bytes = sum(p.stat().st_size for p in closed)
        closed_rows = sum(pq.ParquetFile(p).metadata.num_rows for p in closed)
        per_day = closed_bytes / max(closed_rows, 1) * 86_400 / 1_000_000
        detail = f"{per_day:.1f} MB/día → {per_day * 14 / 1000:.2f} GB en 14 días"
    else:
        pending_bytes = sum(p.stat().st_size for p in pending)
        detail = (
            f"{pending_bytes / 1_000_000:.1f} MB en la hora en curso, todavía sin comprimir. "
            "La previsión llega al cerrar la primera hora"
        )
    checks.append(Check("Crecimiento en disco", Verdict.PASS, detail))
    return checks


def check_health(table: pa.Table) -> list[Check]:
    rows = table.num_rows
    reconnects = max((v or 0) for v in table.column("reconnects").to_pylist())
    stamps = sorted(ts for ts in table.column("ts").to_pylist() if ts is not None)
    span_h = max((stamps[-1] - stamps[0]).total_seconds() / 3600.0, 1e-9)
    per_hour = reconnects / span_h

    checks = [
        Check(
            "Reconexiones",
            _verdict(
                per_hour,
                MAX_RECONNECTS_PER_HOUR,
                MAX_RECONNECTS_PER_HOUR * 3,
                higher_is_better=False,
            ),
            f"{reconnects} en total ({per_hour:.1f}/h)",
        )
    ]

    reasons: Counter[str] = Counter()
    halted = 0
    for raw in table.column("halt_reasons").to_pylist():
        flags = [flag for flag in (raw or "").split(",") if flag]
        if flags:
            halted += 1
        reasons.update(flag.split(":")[0] for flag in flags)
    top = ", ".join(
        f"{name} {count * 100 / max(rows, 1):.0f}%" for name, count in reasons.most_common(4)
    )
    checks.append(
        Check(
            "Halts (§5.2)",
            Verdict.PASS,
            f"{halted * 100 / max(rows, 1):.1f}% de los segundos con alguna bandera"
            + (f" · {top}" if top else ""),
        )
    )

    # El fair es el producto: si no se calcula, los 14 días no responden la pregunta 2.
    for column in (name for name in table.schema.names if name.startswith("fair_band_")):
        present = sum(1 for v in table.column(column).to_pylist() if v is not None)
        share = present / max(rows, 1)
        checks.append(
            Check(
                f"Fair · {column.removeprefix('fair_band_')}",
                _verdict(share, MIN_FAIR_SHARE, MIN_FAIR_SHARE / 2, higher_is_better=True),
                f"{share * 100:.1f}% de los segundos con fair calculado",
            )
        )
    return checks


def check_spots(table: pa.Table) -> list[Check]:
    """Compara los dos spots contra el oráculo. Es lo que decide `spot_source`.

    La magnitud que importa **no es el nivel del basis sino su DISPERSIÓN**. §2.1.c ya lo
    dejó dicho: un basis grande pero estable lo sigue el estimador de §2.1.b sin problema;
    uno que se mueve es un ancla suelta, y el residuo entra entero en `ln(S/K)`.

    Se traduce a céntimos de fair porque es la única unidad comparable contra el colchón de
    3¢ de §3.5. Un dólar de residuo no dice nada por sí solo; 3 céntimos sí.
    """
    chainlink = table.column("chainlink").to_pylist()
    checks: list[Check] = []
    stats: dict[str, dict[str, float]] = {}

    for column, label in (("binance_mid", "binance"), ("coinbase_mid", "coinbase")):
        if column not in table.schema.names:
            continue
        spot = table.column(column).to_pylist()
        diffs = [
            c - s for c, s in zip(chainlink, spot, strict=True) if c is not None and s is not None
        ]
        if len(diffs) < 60:
            checks.append(Check(f"Spot · {label}", Verdict.WARN, "muestras insuficientes"))
            continue
        described = _describe(diffs)
        stats[label] = described
        spread = described["max"] - described["min"]
        checks.append(
            Check(
                f"Spot · {label} vs chainlink",
                Verdict.PASS,
                f"media ${described['mean']:+.2f} · desviación ${described['std']:.2f} · "
                f"rango ${spread:.2f} ({described['n']:.0f} muestras)",
            )
        )

    # Edad de cada feed: el `ticker` de Coinbase emite con cada OPERACIÓN, no con cada
    # cambio del libro. Si en momentos tranquilos se queda quieto, aparece aquí.
    for column, label in (("age_binance_direct", "binance"), ("age_coinbase", "coinbase")):
        if column not in table.schema.names:
            continue
        ages = [a for a in table.column(column).to_pylist() if a is not None]
        if not ages:
            continue
        ordered = sorted(ages)
        p95 = ordered[int(len(ordered) * 0.95)]
        checks.append(
            Check(
                f"Frescura · {label}",
                _verdict(p95, 1.0, 3.0, higher_is_better=False),
                f"p50 {ordered[len(ordered) // 2]:.2f} s · p95 {p95:.2f} s · "
                f"máx {ordered[-1]:.1f} s",
            )
        )

    if len(stats) == 2:
        reference = [c for c in chainlink if c is not None]
        checks.append(_spot_verdict(stats, sum(reference) / len(reference)))
    return checks


def _spot_verdict(stats: dict[str, dict[str, float]], spot: float) -> Check:
    """El veredicto en céntimos de fair: qué fuente deja menos residuo sin corregir.

    La conversión sale de `model/binary.py::delta_per_dollar`, **no de una fórmula copiada
    aquí**. §10 registra que este proyecto arrastró una varianza mal escrita durante toda
    una versión, con hasta 3,9¢ de error; duplicar la función en una herramienta de
    diagnóstico es la forma más fácil de que esa clase de fallo vuelva por la puerta de
    atrás, y encima en el sitio donde menos se mira.
    """
    sigma_s = deannualize(REFERENCE_SIGMA_ANNUAL)
    variance = terminal_variance(sigma_s, REFERENCE_TAU_S, REFERENCE_WINDOW_S)
    per_dollar = delta_per_dollar(spot, spot, variance)

    cents = {name: d["std"] * per_dollar * 100.0 for name, d in stats.items()}
    best = min(cents, key=lambda k: cents[k])
    detail = " · ".join(f"{name} {value:.2f}¢" for name, value in sorted(cents.items()))
    return Check(
        "Residuo del basis en céntimos",
        Verdict.PASS,
        f"{detail} → mejor: {best} (a τ={REFERENCE_TAU_S:.0f} s, σ={REFERENCE_SIGMA_ANNUAL:.0%})",
    )


REFERENCE_SIGMA_ANNUAL = 0.60
REFERENCE_TAU_S = 60.0
REFERENCE_WINDOW_S = 60.0
"""Mismo punto de referencia que §2.1.c usa para el basis: comparable con aquella tabla."""


def check_epochs(data_dir: Path) -> list[Check]:
    """Las épocas viven en `state.db`; el journal JSONL es su copia inmutable."""
    db = data_dir / "state.db"
    if not db.exists():
        return [Check("Épocas", Verdict.FAIL, "no hay state.db")]

    try:
        conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        rows: list[dict[str, Any]] = [
            dict(zip([d[0] for d in cur.description], row, strict=True))
            for cur in [conn.execute("SELECT * FROM epochs")]
            for row in cur.fetchall()
        ]
        conn.close()
    except sqlite3.Error as error:
        return [Check("Épocas", Verdict.FAIL, f"no se pudo leer state.db: {error}")]

    if not rows:
        return [Check("Épocas", Verdict.FAIL, "ninguna época registrada")]

    total = len(rows)
    open_strike = sum(1 for r in rows if r.get("strike_source") == "open")
    resolved = sum(1 for r in rows if r.get("resolved_outcome") in ("Up", "Down"))
    ended = sum(1 for r in rows if r.get("settle_ts"))

    checks = [
        Check(
            "Épocas seguidas",
            _verdict(total, MIN_EPOCHS, MIN_EPOCHS / 2, higher_is_better=True),
            f"{total} épocas, {ended} ya vencidas",
        ),
        Check(
            "Strike en la apertura",
            _verdict(open_strike / total, MIN_STRIKE_OPEN_SHARE, 0.5, higher_is_better=True),
            f"{open_strike}/{total} con strike `open` "
            f"({open_strike * 100 / total:.0f}%); el resto no produce fair",
        ),
    ]
    if ended:
        share = resolved / ended
        checks.append(
            Check(
                "Desenlaces leídos (§8)",
                _verdict(share, MIN_RESOLVED_SHARE, 0.5, higher_is_better=True),
                f"{resolved}/{ended} épocas vencidas con desenlace. "
                "Es la variable dependiente del experimento de la ventana TWAP",
            )
        )

    journal = data_dir / "epochs.jsonl"
    if journal.exists():
        lines = sum(1 for _ in journal.open(encoding="utf-8"))
        checks.append(Check("Journal de épocas", Verdict.PASS, f"{lines} líneas append-only"))
    else:
        checks.append(Check("Journal de épocas", Verdict.WARN, "no existe epochs.jsonl"))
    return checks


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--hours", type=float, default=48.0, help="ventana a analizar")
    parser.add_argument("--json", action="store_true", help="salida legible por máquina")
    args = parser.parse_args()

    table = load_table(args.data_dir, args.hours)
    if table is None or table.num_rows == 0:
        print("No hay datos en la ventana pedida.")
        return 1

    checks = (
        check_coverage(table, args.data_dir)
        + check_health(table)
        + check_spots(table)
        + check_epochs(args.data_dir)
    )
    worst = max(checks, key=lambda c: c.verdict.rank).verdict

    if args.json:
        print(
            json.dumps(
                {
                    "verdict": worst.value,
                    "checks": [vars(c) | {"verdict": c.verdict.value} for c in checks],
                },
                ensure_ascii=False,
                indent=1,
            )
        )
        return 0 if worst is not Verdict.FAIL else 1

    print(f"soak · {args.data_dir} · últimas {args.hours:.0f} h\n")
    for check in checks:
        print(check.render())
    print(f"\n   veredicto: {worst.value}")
    if worst is Verdict.FAIL:
        print("   El contador de 14 días NO empieza. Arregla lo marcado y repite el soak.")
    elif worst is Verdict.WARN:
        print("   Revisa los avisos antes de dar por buena la Fase 0.")
    else:
        print("   Puede empezar el contador de 14 días.")
    return 0 if worst is not Verdict.FAIL else 1


if __name__ == "__main__":
    raise SystemExit(main())
