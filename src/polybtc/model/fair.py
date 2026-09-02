"""Fair value + banda de σ, y to_execution(): única frontera float→Decimal, al tick vigente.

Compone las piezas de §3: `S_eff` (§3.3) → varianza por régimen (§3.2) → N(d2) (§3.1),
promediado sobre la banda de incertidumbre de σ (§3.4).

`FairResult` lleva TODOS los insumos del modelo porque §9 los exige en el log estructurado:
sin σ, basis, `S_eff`, τ y la rama de varianza usada no se puede reconstruir por qué el bot
decidió lo que decidió.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_HALF_EVEN, Decimal

from polybtc.model.binary import d2, delta_per_dollar, fair_from_variance
from polybtc.model.twap import VarianceRegime, effective_spot, terminal_variance, variance_regime

DEFAULT_BAND_REL = 0.30
"""Banda de +-30% sobre sigma (§3.4): el estimador es ruidoso y esto lo hace explicito."""

DEFAULT_BAND_POINTS = 21


@dataclass(frozen=True, slots=True)
class FairResult:
    """Fair value y todos sus insumos. Inmutable: es lo que se loguea y se replica."""

    fair: float
    """N(d2) con la sigma puntual."""

    fair_band: float
    """Promedio del fair sobre la banda de sigma. Es el que se usa para decidir."""

    band_low: float
    """Minimo del fair sobre la banda."""

    band_high: float
    """Maximo del fair sobre la banda."""

    variance: float
    regime: VarianceRegime
    spot_eff: float
    sigma_s: float
    tau_s: float
    window_s: float
    d2: float
    delta_per_dollar: float


def fair_value(
    *,
    spot: float,
    strike: float,
    tau_s: float,
    window_s: float,
    sigma_s: float,
    observed_mean: float | None = None,
    band_rel: float = DEFAULT_BAND_REL,
    band_points: int = DEFAULT_BAND_POINTS,
) -> FairResult:
    """Fair value de la binaria, con la banda de incertidumbre de σ.

    Sobre la banda (§3.4): promediar `N(d2(σ))` sobre un rango de σ es el tratamiento
    correcto de la incertidumbre del estimador, y LEJOS DEL DINERO encoge el fair hacia
    0.50, que es el comportamiento deseado — es ahí donde equivocarse sale caro.

    Cerca del dinero hace lo contrario, y es esperable, no un bug: `d2 ∝ 1/σ` es convexa,
    así que promediar sobre σ aleja ligeramente el fair de 0.50. El efecto está medido en
    0.53¢ como máximo con band_rel=0.30 (τ=300 s, w=30 s, σ=60%), muy por debajo del
    colchón de modelo de 3¢ de §3.5. `tests/test_model.py` lo mantiene acotado por debajo
    de 1¢: si crece, la banda dejó de ser un promedio sobre σ.
    """
    if sigma_s <= 0.0:
        raise ValueError(f"sigma_s debe ser positiva: {sigma_s}")
    if not 0.0 <= band_rel < 1.0:
        raise ValueError(f"band_rel debe estar en [0, 1): {band_rel}")
    if band_points < 1:
        raise ValueError(f"band_points debe ser ≥ 1: {band_points}")

    spot_eff = effective_spot(spot, tau_s, window_s, observed_mean)
    regime = variance_regime(tau_s, window_s)
    variance = terminal_variance(sigma_s, tau_s, window_s)
    fair = fair_from_variance(spot_eff, strike, variance)

    band = [
        fair_from_variance(spot_eff, strike, terminal_variance(sigma, tau_s, window_s))
        for sigma in _sigma_grid(sigma_s, band_rel, band_points)
    ]

    return FairResult(
        fair=fair,
        fair_band=sum(band) / len(band),
        band_low=min(band),
        band_high=max(band),
        variance=variance,
        regime=regime,
        spot_eff=spot_eff,
        sigma_s=sigma_s,
        tau_s=tau_s,
        window_s=window_s,
        d2=d2(spot_eff, strike, variance),
        delta_per_dollar=delta_per_dollar(spot_eff, strike, variance),
    )


def to_execution(fair: float, tick_size: Decimal) -> Decimal:
    """float del modelo → `Decimal` cuantizado al tick vigente.

    **Esta función es la ÚNICA frontera float→Decimal del proyecto (§4).** Un
    `Decimal(str(x))` disperso por el código es exactamente cómo se pierde la frontera.

    `tick_size` debe ser el vigente: el CLOB lo cambia en vivo (`tick_size_change`) y
    rechaza órdenes cuantizadas con un tick obsoleto.
    """
    if not 0.0 <= fair <= 1.0:
        raise ValueError(f"fair fuera de [0, 1]: {fair}")
    if tick_size <= 0:
        raise ValueError(f"tick_size debe ser positivo: {tick_size}")

    exact = Decimal(str(fair))
    ticks = (exact / tick_size).to_integral_value(rounding=ROUND_HALF_EVEN)
    return (ticks * tick_size).quantize(tick_size)


def _sigma_grid(sigma_s: float, band_rel: float, band_points: int) -> list[float]:
    """Rejilla uniforme en σ sobre [σ(1−rel), σ(1+rel)], centrada en σ si es impar.

    Uniforme en σ y no en 1/σ a propósito: medido, promediar sobre precisión empeora el
    anti-encogimiento cerca del dinero de 0.53¢ a 1.93¢.
    """
    if band_points == 1 or band_rel == 0.0:
        return [sigma_s]
    low = sigma_s * (1.0 - band_rel)
    high = sigma_s * (1.0 + band_rel)
    step = (high - low) / (band_points - 1)
    return [low + step * i for i in range(band_points)]
