"""Tests de propiedad del modelo (CLAUDE.md §3.6).

Se escriben ANTES que la implementación (§9): la v1 del documento llevaba un error de
fórmula en la varianza TWAP que ninguna de estas diez propiedades habría dejado pasar.

Dos convenciones deliberadas en este fichero:

1. **Los imports viven dentro de cada test.** Mientras `model/` esté vacío eso da diez
   fallos independientes en vez de un único error de colección. Suben al nivel de módulo
   en cuanto exista la implementación.
2. **El oráculo es independiente de la implementación.** Aquí se usa
   `statistics.NormalDist`; `model/binary.py` usará `math.erfc`. Un error de constante en
   una de las dos no se cancela con la otra.

No se testea "el precio es 0.5312": se testean las propiedades que deben cumplirse
siempre, que es lo que atrapa los errores de signo y de unidades.
"""

from __future__ import annotations

import random
from itertools import pairwise
from math import exp, sqrt
from statistics import NormalDist

import pytest

# Ancla de unidades de §3.0 / §2.3. Todo en SEGUNDOS, nunca en años.
SECONDS_PER_YEAR_REF = 365 * 24 * 3600
SIGMA_ANNUAL_REF = 0.60
SIGMA_S_REF = SIGMA_ANNUAL_REF / sqrt(SECONDS_PER_YEAR_REF)
SPOT_REF = 100_000.0

_NORMAL = NormalDist()


def _cdf(x: float) -> float:
    """N(·) de referencia, por una vía distinta a la de la implementación."""
    return _NORMAL.cdf(x)


def _point_variance(sigma_s: float, tau_s: float) -> float:
    """Liquidación puntual (§3.2, primera fila): σ²·τ."""
    return sigma_s * sigma_s * tau_s


def _window_pending_variance(sigma_s: float, tau_s: float, window_s: float) -> float:
    """Ventana TWAP aún no abierta, τ ≥ w (§3.2, segunda fila): σ²·(τ − 2w/3).

    Escrita a mano y no reutilizada de `model/`: si la implementación reintroduce el
    σ²(τ − w/3) de la v1, este test debe romperse.
    """
    return sigma_s * sigma_s * (tau_s - 2.0 * window_s / 3.0)


def _window_open_variance(sigma_s: float, tau_s: float, window_s: float) -> float:
    """Ventana TWAP ya abierta, τ < w (§3.2, tercera fila): σ²·τ³/(3w²)."""
    return sigma_s * sigma_s * tau_s**3 / (3.0 * window_s * window_s)


# ---------------------------------------------------------------------------
# 1. ATM da ≈ 0.50, ligeramente POR DEBAJO (por el término −var/2)
# ---------------------------------------------------------------------------


def test_atm_fair_is_just_below_half() -> None:
    from polybtc.model.binary import fair_from_variance

    for tau_s, window_s in ((300.0, 30.0), (300.0, 60.0), (300.0, 0.0), (10.0, 30.0)):
        variance = (
            _window_open_variance(SIGMA_S_REF, tau_s, window_s)
            if 0.0 < window_s and tau_s < window_s
            else _window_pending_variance(SIGMA_S_REF, tau_s, window_s)
        )
        fair = fair_from_variance(SPOT_REF, SPOT_REF, variance)

        # Estricto por ambos lados: el −var/2 lo empuja por debajo de 0.50, y no puede
        # alejarse tanto como para salirse de 0.49. Un signo invertido en d2 rompe esto.
        assert 0.49 < fair < 0.50, f"τ={tau_s} w={window_s}: fair={fair}"
        assert fair == pytest.approx(_cdf(-sqrt(variance) / 2.0), abs=1e-12)


# ---------------------------------------------------------------------------
# 2. Monótona creciente en S
# ---------------------------------------------------------------------------


def test_fair_is_monotone_increasing_in_spot() -> None:
    from polybtc.model.binary import fair_from_variance

    regimes = (
        _point_variance(SIGMA_S_REF, 300.0),  # puntual
        _window_pending_variance(SIGMA_S_REF, 300.0, 30.0),  # ventana pendiente
        _window_open_variance(SIGMA_S_REF, 10.0, 30.0),  # ventana abierta
    )
    for variance in regimes:
        spots = [SPOT_REF - 200.0 + 4.0 * i for i in range(101)]
        fairs = [fair_from_variance(s, SPOT_REF, variance) for s in spots]
        for lower, higher in pairwise(fairs):
            assert higher >= lower
        # Y estrictamente creciente donde el fair no está ya saturado en 0 o 1.
        assert fairs[0] < fairs[len(fairs) // 2] < fairs[-1]


# ---------------------------------------------------------------------------
# 3. Converge al indicador 0/1 cuando τ → 0
# ---------------------------------------------------------------------------


def test_fair_converges_to_indicator_as_tau_goes_to_zero() -> None:
    from polybtc.model.binary import fair_from_variance
    from polybtc.model.twap import terminal_variance

    window_s = 30.0
    taus = (20.0, 10.0, 5.0, 1.0, 0.1, 0.01)

    # El empate exacto (S_eff == K) queda fuera a propósito: §8 no ha resuelto si el
    # evento es `>` o `>=`, y a 0.50 ese detalle vale dinero real.
    above = [
        fair_from_variance(SPOT_REF + 50.0, SPOT_REF, terminal_variance(SIGMA_S_REF, t, window_s))
        for t in taus
    ]
    below = [
        fair_from_variance(SPOT_REF - 50.0, SPOT_REF, terminal_variance(SIGMA_S_REF, t, window_s))
        for t in taus
    ]

    for lower, higher in pairwise(above):
        assert higher >= lower
    for higher, lower in pairwise(below):
        assert lower <= higher

    assert above[-1] == pytest.approx(1.0, abs=1e-9)
    assert below[-1] == pytest.approx(0.0, abs=1e-9)


# ---------------------------------------------------------------------------
# 4. var(TWAP) < var(puntual) al mismo horizonte
# ---------------------------------------------------------------------------


def test_twap_variance_is_below_point_variance() -> None:
    from polybtc.model.twap import terminal_variance

    for window_s in (30.0, 60.0):
        for tau_s in (5.0, 10.0, 29.0, 30.0, 60.0, 120.0, 300.0):
            twap = terminal_variance(SIGMA_S_REF, tau_s, window_s)
            point = terminal_variance(SIGMA_S_REF, tau_s, 0.0)

            assert point == pytest.approx(_point_variance(SIGMA_S_REF, tau_s), rel=1e-12)
            # Promediar reduce la varianza: estricto, nunca igual.
            assert twap < point, f"τ={tau_s} w={window_s}: {twap} !< {point}"
            assert twap > 0.0


# ---------------------------------------------------------------------------
# 5. Continuidad de la varianza en τ = w  (el test que la v1 no habría pasado)
# ---------------------------------------------------------------------------


def test_variance_is_continuous_at_tau_equals_window() -> None:
    from polybtc.model.twap import terminal_variance

    for window_s in (30.0, 60.0):
        expected = SIGMA_S_REF * SIGMA_S_REF * window_s / 3.0
        at_boundary = terminal_variance(SIGMA_S_REF, window_s, window_s)
        assert at_boundary == pytest.approx(expected, rel=1e-12)

        # Ambas ramas convergen al mismo valor desde cada lado. La desviación de cada
        # rama es O(3ε/w), así que la tolerancia se encoge con ε: eso es la continuidad,
        # y no lo cumpliría una tolerancia fija por generosa que fuese.
        for eps in (1e-3, 1e-6, 1e-9):
            tol = 4.0 * eps / window_s
            from_below = terminal_variance(SIGMA_S_REF, window_s - eps, window_s)
            from_above = terminal_variance(SIGMA_S_REF, window_s + eps, window_s)
            assert from_below == pytest.approx(expected, rel=tol)
            assert from_above == pytest.approx(expected, rel=tol)
            assert from_below < at_boundary < from_above

        # La v1 (σ²(τ − w/3)) no tenía rama para τ < w y su varianza llegaba a CERO en
        # τ = w/3: el fair colapsaba a 0 o 1 con certeza absoluta a 10 s del cierre.
        assert terminal_variance(SIGMA_S_REF, window_s / 3.0, window_s) > 0.0


# ---------------------------------------------------------------------------
# 6. S_eff con la ventana a mitad da la media ponderada exacta (§3.3)
# ---------------------------------------------------------------------------


def test_effective_spot_is_exact_weighted_mean() -> None:
    from polybtc.model.twap import effective_spot

    window_s = 30.0
    spot = 100_120.0
    observed_mean = 99_940.0

    # Fuera de la ventana el spot pesa todo.
    for tau_s in (30.0, 60.0, 300.0):
        assert effective_spot(spot, tau_s, window_s, None) == spot
        assert effective_spot(spot, tau_s, window_s, observed_mean) == spot

    # Dentro de la ventana, el spot actual solo pesa τ/w.
    for tau_s in (22.5, 15.0, 7.5, 1.0):
        expected = ((window_s - tau_s) * observed_mean + tau_s * spot) / window_s
        assert effective_spot(spot, tau_s, window_s, observed_mean) == pytest.approx(
            expected, rel=1e-12
        )

    # A mitad de ventana es exactamente el punto medio.
    assert effective_spot(spot, window_s / 2.0, window_s, observed_mean) == pytest.approx(
        (observed_mean + spot) / 2.0, rel=1e-12
    )

    # Sin media observada no hay S_eff que calcular: fallar ruidoso, no inventar el spot
    # crudo, que es justamente la señal fantasma que §3.3 viene a eliminar.
    with pytest.raises(ValueError):
        effective_spot(spot, 10.0, window_s, None)


# ---------------------------------------------------------------------------
# 7. La banda de incertidumbre de σ no aleja el fair de 0.50 (§3.4)
# ---------------------------------------------------------------------------


def test_sigma_band_never_moves_fair_away_from_half() -> None:
    """Propiedad 7, formulada sobre lo que es cierto.

    §3.6 la enuncia como «encoge hacia 0.50, nunca lo contrario». Con el promedio honesto
    E_σ[N(d2(σ))] eso es falso cerca del dinero: d2 ∝ 1/σ es convexa, así que promediar
    sobre σ ALEJA el fair de 0.50 (medido: hasta 0.53¢, en τ=300 s, S−K=−50). Solo encoge
    en las colas, que es donde §3.4 dice quererlo. Se testea eso, acotando el efecto
    contrario muy por debajo del colchón de 3¢ de §3.5.
    """
    from polybtc.model.fair import FairResult, fair_value

    def result(strike: float, tau_s: float) -> FairResult:
        return fair_value(
            spot=SPOT_REF,
            strike=strike,
            tau_s=tau_s,
            window_s=30.0,
            sigma_s=SIGMA_S_REF,
            band_rel=0.30,
            band_points=21,
        )

    worst_anti_shrink = 0.0
    for tau_s in (300.0, 60.0):
        for i in range(201):
            strike = 99_800.0 + 2.0 * i
            res = result(strike, tau_s)
            fair, band = res.fair, res.fair_band

            # (a) La banda es un promedio: siempre dentro del rango que promedia.
            assert res.band_low <= band <= res.band_high
            assert res.band_low <= fair <= res.band_high

            drift = abs(band - 0.5) - abs(fair - 0.5)
            worst_anti_shrink = max(worst_anti_shrink, drift)

            # (b) En las colas encoge estrictamente hacia 0.50.
            if abs(fair - 0.5) > 0.45 and 0.0 < fair < 1.0:
                assert abs(band - 0.5) < abs(fair - 0.5)

    # (c) Cerca del dinero el efecto contrario existe, pero acotado y muy por debajo del
    # colchón de modelo de 3¢. Si esto crece, la banda dejó de ser un promedio de σ.
    assert worst_anti_shrink < 0.01, f"anti-encogimiento de {worst_anti_shrink * 100:.3f}¢"


# ---------------------------------------------------------------------------
# 8. delta_per_dollar crece al acercarse el vencimiento (§2.3)
# ---------------------------------------------------------------------------


def test_delta_per_dollar_grows_toward_expiry() -> None:
    from polybtc.model.binary import delta_per_dollar
    from polybtc.model.twap import terminal_variance

    window_s = 30.0
    # La rejilla CRUZA w a propósito: un fallo de continuidad entre ramas la rompe.
    taus = (300.0, 120.0, 60.0, 31.0, 30.0, 29.0, 20.0, 10.0, 5.0)
    deltas = [
        delta_per_dollar(SPOT_REF, SPOT_REF, terminal_variance(SIGMA_S_REF, t, window_s))
        for t in taus
    ]

    for earlier, later in pairwise(deltas):
        assert later > earlier

    # La gamma de un digital ATM explota contra el vencimiento: los últimos segundos son
    # territorio de latencia pura (§2.3), y este número es el que lo demuestra.
    assert deltas[-1] > 20.0 * deltas[0]


# ---------------------------------------------------------------------------
# 9. Cordura de unidades: SEGUNDOS, nunca años (§3.0)
# ---------------------------------------------------------------------------


def test_units_are_seconds_not_years() -> None:
    from polybtc.model.binary import delta_per_dollar
    from polybtc.model.twap import terminal_variance
    from polybtc.model.vol import SECONDS_PER_YEAR, annualize, deannualize

    assert SECONDS_PER_YEAR == SECONDS_PER_YEAR_REF

    sigma_s = deannualize(SIGMA_ANNUAL_REF)
    assert sigma_s == pytest.approx(SIGMA_S_REF, rel=1e-12)
    assert annualize(sigma_s) == pytest.approx(SIGMA_ANNUAL_REF, rel=1e-12)

    # §3.0: con σ=60% anual, 1σ en 300 s sobre BTC a 100.000 son ~$150–300.
    # Confundir años con segundos mueve esto un factor ~5.600.
    one_sigma_usd = SPOT_REF * sigma_s * sqrt(300.0)
    assert 150.0 < one_sigma_usd < 300.0

    # Anclas de §2.3 verificadas contra la fórmula v2 (w=30, S=K=100.000, σ=60%).
    # La fila de τ=10 s de esa tabla NO se usa: dice 0.02044 y la fórmula da 0.06135.
    for tau_s, expected in ((300.0, 0.00223), (60.0, 0.00590)):
        variance = terminal_variance(sigma_s, tau_s, 30.0)
        assert delta_per_dollar(SPOT_REF, SPOT_REF, variance) == pytest.approx(expected, rel=0.01)


# ---------------------------------------------------------------------------
# 10. El estimador de σ recupera una vol conocida inyectada por simulación (§3.4)
# ---------------------------------------------------------------------------


def test_vol_estimator_recovers_injected_sigma() -> None:
    from polybtc.model.vol import EwmaVol, annualize, deannualize

    true_sigma_s = deannualize(SIGMA_ANNUAL_REF)
    rng = random.Random(20260901)

    estimator = EwmaVol(
        half_life_s=600.0,
        floor_s=deannualize(0.05),
        ceiling_s=deannualize(5.00),
    )
    # Se liga a un local a propósito: mypy estrecha el tipo de la property y no invalida
    # ese estrechamiento con las llamadas a update(), así que compararla dos veces haría
    # que diese el segundo assert por inalcanzable.
    ready_at_start = estimator.ready
    assert not ready_at_start

    # Ticks a intervalos IRREGULARES, como los de Binance. Si el estimador trabajase
    # tick a tick en vez de sobre la rejilla de 1 s de §3.4, mediría el ritmo de mensajes
    # y no el mercado, y no recuperaría esta σ.
    now = 0.0
    price = SPOT_REF
    horizon_s = 7200.0
    while now < horizon_s:
        dt = rng.uniform(0.05, 0.40)
        now += dt
        shock = rng.gauss(0.0, 1.0)
        price *= exp(true_sigma_s * sqrt(dt) * shock - 0.5 * true_sigma_s**2 * dt)
        estimator.update(now, price)

    assert estimator.ready
    assert estimator.in_band
    assert estimator.sigma_s == pytest.approx(true_sigma_s, rel=0.15)

    # La lectura anualizada existe solo para logs (§3.0), pero debe ser coherente.
    assert annualize(estimator.sigma_s) == pytest.approx(SIGMA_ANNUAL_REF, rel=0.15)
