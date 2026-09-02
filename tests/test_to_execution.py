"""La frontera float→Decimal de `model/fair.py::to_execution` (§4).

Ninguna de las diez propiedades de §3.6 toca esta función, y es la que convierte un número
del modelo en el precio que se firma y se envía. Es código que toca dinero: §9 pide tests
antes que features precisamente aquí.

Lo que se protege:

- que el resultado sea `Decimal` **cuantizado al tick vigente**, porque el CLOB rechaza
  órdenes con un tick obsoleto y el tick cambia en vivo (`tick_size_change`);
- que la basura binaria del `float` del modelo muera en esta función y no viaje al CLOB;
- que la conversión falle ruidosa ante entradas imposibles en vez de firmar un precio raro.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

TICK_CENT = Decimal("0.01")
TICK_MILLI = Decimal("0.001")


def test_to_execution_quantizes_to_the_live_tick() -> None:
    from polybtc.model.fair import to_execution

    for tick in (TICK_CENT, TICK_MILLI):
        for fair in (0.0, 0.0004, 0.12345, 0.5, 0.499_999, 0.87654, 1.0):
            price = to_execution(fair, tick)

            assert isinstance(price, Decimal)
            # Múltiplo exacto del tick, y con el mismo número de decimales que el tick:
            # un precio con más decimales de los que admite el mercado es un rechazo.
            assert price % tick == 0
            assert price.as_tuple().exponent == tick.as_tuple().exponent
            assert Decimal(0) <= price <= Decimal(1)
            # El redondeo nunca puede desplazar el precio más de medio tick.
            assert abs(price - Decimal(str(fair))) <= tick / 2

    # Empates resueltos por half-even, que es determinista en ambos sentidos.
    assert to_execution(0.125, TICK_CENT) == Decimal("0.12")
    assert to_execution(0.135, TICK_CENT) == Decimal("0.14")


def test_to_execution_kills_binary_float_noise() -> None:
    from polybtc.model.fair import to_execution

    # El fair llega del modelo en float y arrastra el ruido binario de todo su cálculo.
    noisy = 0.1 + 0.2  # 0.30000000000000004
    assert noisy != 0.3

    price = to_execution(noisy, TICK_CENT)
    assert price == Decimal("0.30")
    assert str(price) == "0.30"

    # Esta es la única función autorizada a hacer la conversión (§4). Si el ruido saliera
    # de aquí, cada `Decimal(str(x))` disperso por el código lo volvería a colar.
    assert price != Decimal(noisy)


def test_to_execution_follows_a_live_tick_size_change() -> None:
    from polybtc.model.fair import to_execution

    fair = 0.4567

    coarse = to_execution(fair, TICK_CENT)
    fine = to_execution(fair, TICK_MILLI)

    assert coarse == Decimal("0.46")
    assert fine == Decimal("0.457")
    # El tick vigente manda: el mismo fair produce precios distintos y cada uno es válido
    # solo contra su propio tick.
    assert coarse % TICK_CENT == 0
    assert fine % TICK_MILLI == 0
    assert abs(fine - Decimal(str(fair))) < abs(coarse - Decimal(str(fair)))


def test_to_execution_rejects_impossible_inputs() -> None:
    from polybtc.model.fair import to_execution

    # Un fair fuera de [0, 1] no es una probabilidad: es un bug del modelo. Fallar aquí
    # es barato; firmarlo y enviarlo, no.
    for fair in (-0.01, 1.01, 2.0):
        with pytest.raises(ValueError):
            to_execution(fair, TICK_CENT)

    for tick in (Decimal(0), Decimal("-0.01")):
        with pytest.raises(ValueError):
            to_execution(0.5, tick)
