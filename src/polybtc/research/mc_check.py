"""Verificación independiente de `model/twap.py::terminal_variance` por Monte Carlo.

Los tests de propiedad de §3.6 comprueban que el modelo es coherente **consigo mismo**:
continuidad en τ=w, monotonía, signos. No pueden comprobar que la fórmula describa la
física, porque están escritos a partir de la misma fórmula. Eso es exactamente lo que
dejó pasar el `σ²(τ − w/3)` de la v1 de CLAUDE.md durante meses.

Este script cierra ese hueco: simula GBM, construye el TWAP como lo construye el oráculo
—promedio del spot sobre la ventana `w` que termina en `T`— y compara la varianza empírica
de `ln(TWAP)` contra `terminal_variance()`. Debe coincidir dentro del 2%.

La columna "v1" muestra qué habría dado la fórmula incorrecta, para que se vea que la
comprobación tiene dientes y no está pasando por casualidad.

Uso:

    uv run python -m polybtc.research.mc_check
    uv run python -m polybtc.research.mc_check --paths 50000 --dt 0.1
"""

from __future__ import annotations

import argparse
import math
import sys
from dataclasses import dataclass

import numpy as np
from numpy.typing import NDArray

from polybtc.model.twap import VarianceRegime, terminal_variance, variance_regime
from polybtc.model.vol import deannualize

# Un caso por rama, más un control.
#
# - (300, 0) es el control: liquidación puntual. No prueba el TWAP, prueba el arnés. Si
#   `w=0` no da σ²·τ, el error está en la simulación y las demás filas son ruido.
# - Los cuatro con τ ≥ w cubren la rama `window_pending`, incluido el borde τ=w.
# - Los tres con τ < w cubren `window_open`, la rama que la v1 no tenía y que gobierna
#   los últimos segundos, donde la gamma explota (§2.3). Verificarla exige simular
#   también el `S_eff` de §3.3: sin él, la comparación no significa nada.
CASES: tuple[tuple[float, float], ...] = (
    (300.0, 0.0),
    (300.0, 30.0),
    (120.0, 30.0),
    (60.0, 30.0),
    (60.0, 60.0),
    (20.0, 30.0),
    (10.0, 30.0),
    (30.0, 60.0),
)

TOLERANCE = 0.02
"""El 2% que exige el criterio de salida."""

MIN_PATHS = 80_000
"""Trayectorias mínimas para que el ruido (sqrt(2/N)) resuelva el 2% con margen de 4x."""


@dataclass(frozen=True)
class CaseResult:
    tau_s: float
    window_s: float
    empirical: float
    theoretical: float
    v1_formula: float
    std_error_rel: float

    @property
    def error_rel(self) -> float:
        return self.empirical / self.theoretical - 1.0

    @property
    def v1_error_rel(self) -> float:
        return self.v1_formula / self.empirical - 1.0

    @property
    def passed(self) -> bool:
        """Falla solo si se sale del 2% **y** del ruido de Monte Carlo.

        El criterio de record es el 2%, pero una desviación dentro de tres errores
        estándar no distingue un bug de la suerte del muestreo. Marcarla como fallo
        mandaría a cazar un fantasma; para eso está el aviso de corrida infrapotenciada.
        """
        return abs(self.error_rel) <= max(TOLERANCE, 3.0 * self.std_error_rel)


def _v1_variance(sigma_s: float, tau_s: float, window_s: float) -> float:
    """La fórmula de la v1: σ²(τ − w/3), sin rama para τ < w. Solo para contraste."""
    return sigma_s * sigma_s * (tau_s - window_s / 3.0)


def _chunks(paths: int, steps: int, max_cells: int) -> list[int]:
    chunk = max(1, min(paths, max_cells // max(1, steps)))
    sizes = [chunk] * (paths // chunk)
    if paths % chunk:
        sizes.append(paths % chunk)
    return sizes


def simulate_log_twap(
    sigma_s: float,
    tau_s: float,
    window_s: float,
    paths: int,
    dt: float,
    rng: np.random.Generator,
    max_cells: int = 20_000_000,
) -> NDArray[np.float64]:
    """`ln(TWAP / S_0)` por trayectoria, con la ventana TWAP aún sin abrir (τ ≥ w).

    El tramo previo a la ventana se resuelve con un solo normal exacto —es un browniano
    y no necesita discretizarse—, así que la única fuente de sesgo numérico es el paso
    `dt` dentro de la ventana, que es donde vive el promedio.
    """
    pre_window_s = tau_s - window_s
    drift = -0.5 * sigma_s * sigma_s
    steps = max(1, round(window_s / dt))

    samples: list[NDArray[np.float64]] = []
    for size in _chunks(paths, steps, max_cells):
        # Hasta el inicio de la ventana: un único salto, exacto.
        log_price = rng.normal(
            loc=drift * pre_window_s,
            scale=sigma_s * math.sqrt(pre_window_s) if pre_window_s > 0.0 else 0.0,
            size=size,
        )

        if window_s == 0.0:
            samples.append(log_price.astype(np.float64))
            continue

        # Dentro de la ventana sí hace falta el camino: el TWAP promedia el spot, no su log.
        increments = rng.normal(loc=drift * dt, scale=sigma_s * math.sqrt(dt), size=(size, steps))
        path = log_price[:, None] + np.cumsum(increments, axis=1)
        twap = np.exp(path).mean(axis=1)
        samples.append(np.log(twap).astype(np.float64))

    return np.concatenate(samples)


def simulate_log_twap_open_window(
    sigma_s: float,
    tau_s: float,
    window_s: float,
    paths: int,
    dt: float,
    rng: np.random.Generator,
    max_cells: int = 20_000_000,
) -> NDArray[np.float64]:
    """`ln(TWAP / S_eff)` por trayectoria, con la ventana ya corriendo (τ < w).

    Aquí la mitad del asunto es que **parte de la media que liquidará ya es un hecho
    consumado**. Se toma el caso neutro `media_observada = S_0`, que por §3.3 hace
    `S_eff = S_0`, y solo los `τ` segundos que faltan son aleatorios:

        TWAP = [(w − τ)·media_observada + τ·media(S sobre lo que queda)] / w

    Comparar contra `terminal_variance` en esta rama verifica §3.2 y §3.3 a la vez: si el
    peso `τ/w` estuviera mal, la varianza empírica no cuadraría aunque la fórmula de la
    varianza fuese correcta.
    """
    drift = -0.5 * sigma_s * sigma_s
    steps = max(1, round(tau_s / dt))
    observed_weight = (window_s - tau_s) / window_s
    remaining_weight = tau_s / window_s

    samples: list[NDArray[np.float64]] = []
    for size in _chunks(paths, steps, max_cells):
        increments = rng.normal(loc=drift * dt, scale=sigma_s * math.sqrt(dt), size=(size, steps))
        path = np.cumsum(increments, axis=1)
        remaining_mean = np.exp(path).mean(axis=1)
        # S_0 = 1 y media_observada = 1, luego S_eff = 1 y el log es directo.
        twap = observed_weight + remaining_weight * remaining_mean
        samples.append(np.log(twap).astype(np.float64))

    return np.concatenate(samples)


def run_case(
    sigma_s: float,
    tau_s: float,
    window_s: float,
    paths: int,
    dt: float,
    rng: np.random.Generator,
) -> CaseResult:
    simulate = (
        simulate_log_twap_open_window
        if variance_regime(tau_s, window_s) is VarianceRegime.WINDOW_OPEN
        else simulate_log_twap
    )
    log_twap = simulate(sigma_s, tau_s, window_s, paths, dt, rng)
    return CaseResult(
        tau_s=tau_s,
        window_s=window_s,
        empirical=float(np.var(log_twap, ddof=1)),
        theoretical=terminal_variance(sigma_s, tau_s, window_s),
        v1_formula=_v1_variance(sigma_s, tau_s, window_s),
        # Error estándar relativo de una varianza muestral gaussiana: sqrt(2/N).
        std_error_rel=math.sqrt(2.0 / paths),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--paths", type=int, default=300_000)
    parser.add_argument("--dt", type=float, default=0.05, help="paso dentro de la ventana, en s")
    parser.add_argument("--sigma-annual", type=float, default=0.60)
    parser.add_argument("--seed", type=int, default=20260902)
    args = parser.parse_args(argv)

    sigma_s = deannualize(args.sigma_annual)
    rng = np.random.default_rng(args.seed)

    print(
        f"Monte Carlo de terminal_variance() — {args.paths:,} trayectorias, "
        f"dt={args.dt}s dentro de la ventana, sigma={args.sigma_annual:.0%} anual"
    )
    std_error_rel = math.sqrt(2.0 / args.paths)
    print(f"Error estandar de la varianza muestral: +-{std_error_rel * 100:.2f}%")
    if args.paths < MIN_PATHS:
        print(
            f"AVISO: corrida infrapotenciada. Con {args.paths:,} trayectorias el ruido no "
            f"resuelve el {TOLERANCE:.0%} con margen; usa --paths {MIN_PATHS:,} o mas para "
            "un veredicto de record."
        )
    print()

    header = f"{'tau':>6} {'w':>5} {'regimen':>15} {'empirica':>12} {'teorica':>12} {'error':>8}"
    header += f" {'v1 habria dado':>16} {'':>4}"
    print(header)
    print("-" * len(header))

    results = [run_case(sigma_s, tau, w, args.paths, args.dt, rng) for tau, w in CASES]
    for result in results:
        regime = variance_regime(result.tau_s, result.window_s).value
        mark = "OK" if result.passed else "FALLA"
        print(
            f"{result.tau_s:6.0f} {result.window_s:5.0f} {regime:>15} "
            f"{result.empirical:12.4e} {result.theoretical:12.4e} "
            f"{result.error_rel * 100:+7.2f}% {result.v1_error_rel * 100:+15.1f}% {mark:>5}"
        )

    failed = [r for r in results if not r.passed]
    print()
    if failed:
        print(f"FALLA: {len(failed)} de {len(results)} casos fuera del {TOLERANCE:.0%}.")
        print("Hay un bug de modelo. Arreglalo aqui, no despues de grabar 14 dias de datos.")
        return 1

    worst = max(results, key=lambda r: abs(r.error_rel))
    print(
        f"OK: los {len(results)} casos dentro del {TOLERANCE:.0%}. "
        f"Peor desviacion {worst.error_rel * 100:+.2f}% (tau={worst.tau_s:.0f}s, "
        f"w={worst.window_s:.0f}s)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
