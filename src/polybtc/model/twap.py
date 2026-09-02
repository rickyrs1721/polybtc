"""Varianza terminal por régimen de ventana TWAP y spot efectivo S_eff (§3.2, §3.3).

Todo en SEGUNDOS y en `float` (§3.0, §4). `sigma_s` es la vol POR SEGUNDO: pasarle una
vol anual mete un factor ~5.600 en el fair y no da ningún error.

La variable terminal de estos mercados no es `S_T` sino el promedio del spot sobre la
ventana `w` (30 o 60 s) que termina en `T`. Promediar reduce la varianza, y cuánto la
reduce depende de si la ventana ya se abrió.
"""

from __future__ import annotations

from enum import StrEnum


class VarianceRegime(StrEnum):
    """Rama de §3.2 en uso. §9 exige loguearla: sin ella no se reconstruye una decisión."""

    POINT = "point"
    """Liquidacion puntual (w = 0): var = sigma^2 * tau."""

    WINDOW_PENDING = "window_pending"
    """La ventana TWAP aun no se abre (tau >= w): var = sigma^2 * (tau - 2w/3)."""

    WINDOW_OPEN = "window_open"
    """La ventana TWAP ya esta corriendo (tau < w): var = sigma^2 * tau^3 / (3w^2)."""


def variance_regime(tau_s: float, window_s: float) -> VarianceRegime:
    """Rama de varianza aplicable a este `τ` y esta ventana."""
    _validate_horizon(tau_s, window_s)
    if window_s == 0.0:
        return VarianceRegime.POINT
    if tau_s >= window_s:
        return VarianceRegime.WINDOW_PENDING
    return VarianceRegime.WINDOW_OPEN


def terminal_variance(sigma_s: float, tau_s: float, window_s: float) -> float:
    """Varianza del log del valor de liquidación, en las unidades de `ln(S)`.

    Derivación de la rama `τ ≥ w`: de ahora al inicio de la ventana se acumula σ²(τ−w),
    y promediar dentro de la ventana añade σ²·w/3 (resultado clásico de opción asiática).
    Total σ²(τ − 2w/3).

    La v1 de CLAUDE.md decía σ²(τ − w/3) y no tenía rama para `τ < w`: sobreestimaba la
    varianza hasta un +98,8% y hacía que el fair colapsase a 0 o 1 con certeza absoluta a
    10 s del cierre. No reintroducir.
    """
    if sigma_s < 0.0:
        raise ValueError(f"sigma_s no puede ser negativa: {sigma_s}")
    regime = variance_regime(tau_s, window_s)
    if regime is VarianceRegime.POINT:
        horizon = tau_s
    elif regime is VarianceRegime.WINDOW_PENDING:
        horizon = tau_s - 2.0 * window_s / 3.0
    else:
        horizon = tau_s**3 / (3.0 * window_s * window_s)
    return sigma_s * sigma_s * horizon


def effective_spot(
    spot: float,
    tau_s: float,
    window_s: float,
    observed_mean: float | None,
) -> float:
    """Spot efectivo: con la ventana abierta, parte de la media que liquidará ya es un
    hecho consumado y el spot actual solo pesa `τ/w` (§3.3).

    Sin esto, en los últimos segundos se compara el spot crudo contra el strike y se ven
    movimientos que el valor de liquidación ya no puede seguir. Son señales fantasma, y
    llegan justo cuando la gamma es máxima (§2.3).

    `observed_mean` debe venir del propio feed TWAP de RTDS; el acumulador local es solo
    fallback. Con la ventana abierta y sin media observada esto lanza `ValueError` en vez
    de devolver el spot crudo: caer al spot crudo es exactamente el bug que §3.3 elimina.
    """
    _validate_horizon(tau_s, window_s)
    if spot <= 0.0:
        raise ValueError(f"spot debe ser positivo: {spot}")

    if window_s == 0.0 or tau_s >= window_s:
        return spot

    if observed_mean is None:
        raise ValueError(
            f"ventana TWAP abierta (τ={tau_s}s < w={window_s}s) sin media observada: "
            "no hay S_eff que calcular"
        )
    if observed_mean <= 0.0:
        raise ValueError(f"observed_mean debe ser positiva: {observed_mean}")

    return ((window_s - tau_s) * observed_mean + tau_s * spot) / window_s


def _validate_horizon(tau_s: float, window_s: float) -> None:
    if tau_s < 0.0:
        raise ValueError(f"tau_s no puede ser negativo: {tau_s}")
    if window_s < 0.0:
        raise ValueError(f"window_s no puede ser negativa: {window_s}")
