"""El estimador de basis Binance↔Chainlink (§2.1.b).

Tampoco lo cubre ninguna de las diez propiedades de §3.6, y es el módulo cuyo fallo
«parece una estrategia rentable durante dos días y luego devuelve todo con intereses»:
un basis sin corregir entra íntegro en `ln(S/K)` como un sesgo constante y siempre en la
misma dirección.

Se protegen las cuatro cosas que lo hacen útil — que converja al basis real, que un tick
corrupto no lo envenene, que no se deje usar antes de converger, y que su half-life sea de
tiempo y no de mensajes — más la de arriba: que la corrección elimine de verdad el sesgo.
"""

from __future__ import annotations

import random
from math import exp, log, sqrt

import pytest

# ~$30 sobre BTC a 100.000: el orden de magnitud del desanclaje de USDT más la prima por
# plaza que describe §2.1.b.
TRUE_LOG_BASIS = 3e-4
BINANCE_REF = 100_000.0


def _chainlink_from(binance: float, log_basis: float = TRUE_LOG_BASIS) -> float:
    return binance * exp(log_basis)


def test_basis_recovers_a_constant_offset() -> None:
    from polybtc.model.basis import BasisEstimator

    rng = random.Random(20260902)
    estimator = BasisEstimator(half_life_s=60.0, min_samples=30)

    # Se arranca deliberadamente desde una observación sesgada: si el estimador solo
    # copiase la última muestra, este test no distinguiría nada. Se le dan 20 half-lives
    # para olvidarla (a 6,7 half-lives todavía queda un 1% de ese arranque, que a este
    # tamaño son 7,7e-5 de sesgo: el estimador es lento a propósito).
    estimator.update(0.0, _chainlink_from(BINANCE_REF, 0.008), BINANCE_REF)

    for i in range(1, 1201):
        binance = BINANCE_REF * exp(rng.gauss(0.0, 1e-3))
        noise = rng.gauss(0.0, 1e-4)
        estimator.update(float(i), _chainlink_from(binance, TRUE_LOG_BASIS + noise), binance)

    assert estimator.ready
    assert estimator.log_basis == pytest.approx(TRUE_LOG_BASIS, abs=3e-5)
    assert estimator.basis_ratio == pytest.approx(exp(TRUE_LOG_BASIS), abs=3e-5)
    assert estimator.to_oracle(BINANCE_REF) == pytest.approx(_chainlink_from(BINANCE_REF), abs=5.0)


def test_basis_clamps_a_corrupt_tick() -> None:
    from polybtc.model.basis import BasisEstimator

    clamp = 0.01
    estimator = BasisEstimator(half_life_s=600.0, clamp=clamp, min_samples=2)
    for i in range(60):
        estimator.update(float(i), _chainlink_from(BINANCE_REF), BINANCE_REF)

    settled = estimator.log_basis
    # Cada lectura de la property se liga a un local: mypy la estrecha y no invalida ese
    # estrechamiento con las llamadas a update(), así que leerla dos veces daría la
    # segunda por inalcanzable.
    clamped_while_healthy = estimator.last_observation_clamped
    assert not clamped_while_healthy

    # Un feed corrupto que dice que BTC vale el décuplo. Sin clamp, ln(10) = 2,3 barrería
    # el estimador; con clamp la observación aporta como mucho `clamp`.
    estimator.update(60.0, BINANCE_REF * 10.0, BINANCE_REF)
    clamped_on_corrupt = estimator.last_observation_clamped
    assert clamped_on_corrupt
    assert abs(estimator.log_basis - settled) <= clamp

    # Y el estimador se recupera solo cuando vuelven los datos buenos.
    for i in range(61, 1300):
        estimator.update(float(i), _chainlink_from(BINANCE_REF), BINANCE_REF)
    clamped_after_recovery = estimator.last_observation_clamped
    assert not clamped_after_recovery
    assert estimator.log_basis == pytest.approx(TRUE_LOG_BASIS, abs=1e-5)


def test_basis_refuses_to_be_used_before_converging() -> None:
    from polybtc.model.basis import BasisEstimator

    min_samples = 30
    estimator = BasisEstimator(half_life_s=600.0, min_samples=min_samples)

    # §5.2: basis sin converger → no cotizar. Devolver un número a medias sería peor que
    # no devolver ninguno, así que esto lanza en vez de adivinar.
    with pytest.raises(RuntimeError):
        estimator.to_oracle(BINANCE_REF)

    for i in range(min_samples - 1):
        estimator.update(float(i), _chainlink_from(BINANCE_REF), BINANCE_REF)
    ready_just_before = estimator.ready
    assert not ready_just_before
    with pytest.raises(RuntimeError):
        estimator.to_oracle(BINANCE_REF)

    estimator.update(float(min_samples), _chainlink_from(BINANCE_REF), BINANCE_REF)
    assert estimator.ready
    assert estimator.to_oracle(BINANCE_REF) > 0.0

    # Precios imposibles fallan ruidoso: un 0 en un feed no puede convertirse en un basis.
    with pytest.raises(ValueError):
        estimator.update(500.0, 0.0, BINANCE_REF)
    with pytest.raises(ValueError):
        estimator.update(500.0, BINANCE_REF, -1.0)
    with pytest.raises(ValueError):
        estimator.to_oracle(0.0)


def test_basis_half_life_is_time_not_message_count() -> None:
    from polybtc.model.basis import BasisEstimator

    half_life_s = 600.0
    old, new = 0.001, 0.005

    # Tras exactamente una half-life, el valor viejo pesa la mitad. Es la definición.
    estimator = BasisEstimator(half_life_s=half_life_s, min_samples=1)
    estimator.update(0.0, _chainlink_from(BINANCE_REF, old), BINANCE_REF)
    estimator.update(half_life_s, _chainlink_from(BINANCE_REF, new), BINANCE_REF)
    assert estimator.log_basis == pytest.approx((old + new) / 2.0, rel=1e-12)

    # Con la misma observación repetida, el resultado depende SOLO del tiempo total
    # transcurrido, no de cuántos mensajes llegaron: los decaimientos se multiplican.
    # Una half-life contada en ticks mediría el ritmo de mensajes de los feeds — que son
    # distintos e irregulares entre Binance y Chainlink — en vez de los diez minutos.
    dense = BasisEstimator(half_life_s=half_life_s, min_samples=1)
    sparse = BasisEstimator(half_life_s=half_life_s, min_samples=1)
    dense.update(0.0, _chainlink_from(BINANCE_REF, old), BINANCE_REF)
    sparse.update(0.0, _chainlink_from(BINANCE_REF, old), BINANCE_REF)
    for i in range(1, 11):  # 10 mensajes, uno cada 120 s
        dense.update(i * 120.0, _chainlink_from(BINANCE_REF, new), BINANCE_REF)
    for i in range(1, 3):  # 2 mensajes, uno cada 600 s
        sparse.update(i * 600.0, _chainlink_from(BINANCE_REF, new), BINANCE_REF)

    assert dense.samples != sparse.samples
    assert dense.log_basis == pytest.approx(sparse.log_basis, rel=1e-12)

    # Y ese valor común es exactamente el que impone la half-life: 1200 s son dos
    # half-lives, así que del valor viejo queda un cuarto.
    assert dense.log_basis == pytest.approx(0.25 * old + 0.75 * new, rel=1e-9)

    # El contraste: mismos 10 mensajes en 1 s apenas mueven el estimador.
    fast = BasisEstimator(half_life_s=half_life_s, min_samples=1)
    fast.update(0.0, _chainlink_from(BINANCE_REF, old), BINANCE_REF)
    for i in range(1, 11):
        fast.update(i * 0.1, _chainlink_from(BINANCE_REF, new), BINANCE_REF)
    assert fast.samples == dense.samples
    assert fast.log_basis == pytest.approx(old, abs=1e-5)


def test_basis_correction_removes_the_bias_in_log_moneyness() -> None:
    """La razón de existir del módulo, medida en centavos de fair (§2.1.b)."""
    from polybtc.model.basis import BasisEstimator
    from polybtc.model.binary import fair_from_variance
    from polybtc.model.twap import terminal_variance

    strike = BINANCE_REF  # el strike vive en unidades del oráculo
    sigma_s = 0.60 / sqrt(365 * 24 * 3600)
    variance = terminal_variance(sigma_s, 300.0, 30.0)

    estimator = BasisEstimator(half_life_s=600.0, min_samples=30)
    for i in range(200):
        estimator.update(float(i), _chainlink_from(BINANCE_REF), BINANCE_REF)

    oracle_spot = _chainlink_from(BINANCE_REF)
    fair_true = fair_from_variance(oracle_spot, strike, variance)
    fair_raw = fair_from_variance(BINANCE_REF, strike, variance)
    fair_fixed = fair_from_variance(estimator.to_oracle(BINANCE_REF), strike, variance)

    # El sesgo crudo es constante y siempre del mismo signo, y a este horizonte vale
    # varios centavos: MÁS que la fee taker de 1,75¢ y más que el colchón de 3¢ de §3.5.
    # Un colchón no protege de un sesgo que lo excede.
    assert abs(fair_true - fair_raw) > 0.03
    # Corregido, el error residual es despreciable frente a ese colchón.
    assert abs(fair_true - fair_fixed) < 0.005

    # Y el basis en unidades de precio es del orden que describe §2.1.b: decenas de USD.
    assert 10.0 < estimator.to_oracle(BINANCE_REF) - BINANCE_REF < 100.0
    assert log(estimator.basis_ratio) > 0.0
