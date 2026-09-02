"""Binaria cash-or-nothing: fair = N(d2) y delta_per_dollar como diagnóstico (§3.1, §2.3).

Bajo GBM con drift ≈ 0 (correcto en horizontes de minutos):

    d2   = [ln(S_eff / K) − var/2] / sqrt(var)
    fair = N(d2)

`var` viene de `twap.terminal_variance` y `S_eff` de `twap.effective_spot`. Aquí no se
sabe nada de ventanas: esta capa solo traduce (S_eff, K, var) a probabilidad.
"""

from __future__ import annotations

import math

_SQRT_2 = math.sqrt(2.0)
_INV_SQRT_2PI = 1.0 / math.sqrt(2.0 * math.pi)

# Empate exacto (S_eff == K) con varianza nula. §8 no ha resuelto si el evento es `>` o
# `>=`; 0.5 es la respuesta honesta mientras no se lea la descripción del mercado. En una
# binaria cotizada a 0.50 el empate vale dinero real: resolverlo antes de operar en vivo.
_TIE_PROBABILITY = 0.5


def norm_cdf(x: float) -> float:
    """N(·), la CDF normal estándar, vía `erfc` (estable en las colas)."""
    return 0.5 * math.erfc(-x / _SQRT_2)


def norm_pdf(x: float) -> float:
    """φ(·), la densidad normal estándar."""
    return _INV_SQRT_2PI * math.exp(-0.5 * x * x)


def d2(spot_eff: float, strike: float, variance: float) -> float:
    """[ln(S_eff/K) − var/2] / sqrt(var). Diverge a ±inf cuando la varianza se anula."""
    _validate(spot_eff, strike, variance)
    moneyness = math.log(spot_eff / strike)
    if variance == 0.0:
        if moneyness == 0.0:
            return 0.0
        return math.inf if moneyness > 0.0 else -math.inf
    return (moneyness - variance / 2.0) / math.sqrt(variance)


def fair_from_variance(spot_eff: float, strike: float, variance: float) -> float:
    """Fair value de la binaria «¿S_eff ≥ K?», en [0, 1].

    ATM el fair queda ligeramente POR DEBAJO de 0.50: el término −var/2 es la corrección
    de Itô, no un error de signo.
    """
    _validate(spot_eff, strike, variance)
    if variance == 0.0:
        if spot_eff == strike:
            return _TIE_PROBABILITY
        return 1.0 if spot_eff > strike else 0.0
    return norm_cdf(d2(spot_eff, strike, variance))


def delta_per_dollar(spot_eff: float, strike: float, variance: float) -> float:
    """Cuánto se mueve el fair si el subyacente se mueve $1 — el diagnóstico de §2.3.

    Es φ(d2)/(S·√var). La gamma de un digital ATM explota contra el vencimiento: a 10 s
    del cierre y con ventana de 30 s vale ~0.061, es decir 614 ticks de 0.001 por cada
    $10 de BTC. Ese número, no la intuición, decide en qué épocas se cotiza; por encima
    de cierto valor la franja es territorio de latencia pura y se pierde por construcción.
    """
    _validate(spot_eff, strike, variance)
    if variance == 0.0:
        return math.inf
    return norm_pdf(d2(spot_eff, strike, variance)) / (spot_eff * math.sqrt(variance))


def _validate(spot_eff: float, strike: float, variance: float) -> None:
    if spot_eff <= 0.0:
        raise ValueError(f"spot_eff debe ser positivo: {spot_eff}")
    if strike <= 0.0:
        raise ValueError(f"strike debe ser positivo: {strike}")
    if variance < 0.0:
        raise ValueError(f"variance no puede ser negativa: {variance}")
