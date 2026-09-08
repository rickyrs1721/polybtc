"""Descubrimiento de la época: frontera, slug y desenlace. Sin red.

Estos tests fijan contra el modelo del SDK oficial lo que se midió contra la API el
2026-09-08, que corrigió tres supuestos del proyecto a la vez:

1. El slug es `<familia>-<inicio en segundos Unix>`, y el final es `inicio + duración`.
2. `state.start_date` es cuándo se **creó** el mercado (~un día antes), no su apertura.
3. Una época recién vencida puede figurar `closed=False` con el libro ya en 0.005/0.995:
   leer ese precio como desenlace contaminaría la variable dependiente del experimento
   de §8.

El fixture es un `Market` validado por el propio modelo del SDK, no un objeto simulado. Así
un cambio de nombre de campo en el SDK rompe estos tests en vez de romper la grabación.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest
from polymarket.models.gamma.market import Market

from polybtc.feeds.discovery import (
    OUTCOME_DOWN,
    OUTCOME_UP,
    current_of,
    epoch_from_market,
    epoch_start_unix,
    resolved_outcome,
    slug_for,
)

EPOCH_START = 1788903900
"""2026-09-08 21:45:00 UTC. Su mercado vence a las 22:00:00, medido contra la API."""


def market(**overrides: Any) -> Market:
    """Forma real de la respuesta de Gamma, recortada a lo que este módulo lee."""
    raw: dict[str, Any] = {
        "id": "1",
        "slug": slug_for("btc-updown-15m", EPOCH_START),
        "condition_id": "0x" + "1f" * 32,
        "question": "Bitcoin Up or Down - September 8, 5:45PM-6:00PM ET",
        "state": {
            "active": True,
            "closed": False,
            "archived": False,
            "accepting_orders": True,
            "start_date": "2026-09-07T22:01:23.770947Z",
            "end_date": "2026-09-08T22:00:00Z",
        },
        "outcomes": {
            "yes": {"label": "Up", "token_id": "111", "price": "0.395"},
            "no": {"label": "Down", "token_id": "222", "price": "0.605"},
        },
        "metrics": {},
        "prices": {},
        "trading": {"minimum_order_size": "5", "minimum_tick_size": "0.01"},
        "resolution": {},
        "rewards": {},
        "sports": {},
        "events": [],
        "tags": [],
    }
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(raw.get(key), dict):
            raw[key] = {**raw[key], **value}
        else:
            raw[key] = value
    return Market.model_validate(raw)


# ---------------------------------------------------------------------------
# Frontera de la época
# ---------------------------------------------------------------------------


def test_el_slug_se_construye_desde_la_rejilla_de_la_familia() -> None:
    """Construirlo es lo que evita paginar: una petición y determinista."""
    now = datetime(2026, 9, 8, 21, 48, 58, tzinfo=UTC)
    assert epoch_start_unix(now, 900.0) == EPOCH_START
    assert slug_for("btc-updown-15m", EPOCH_START) == "btc-updown-15m-1788903900"


def test_la_apertura_sale_del_slug_no_de_start_date() -> None:
    """`state.start_date` es la creación del mercado: llega con ~un día de antelación."""
    epoch = epoch_from_market(market(), epoch_seconds=900.0)
    assert epoch is not None
    assert epoch.start == datetime(2026, 9, 8, 21, 45, tzinfo=UTC)
    assert epoch.end == datetime(2026, 9, 8, 22, 0, tzinfo=UTC)
    assert epoch.duration_s == 900.0
    assert epoch.created_at == datetime(2026, 9, 7, 22, 1, 23, 770947, tzinfo=UTC)
    assert (epoch.start - epoch.created_at).total_seconds() > 80_000, "creado un día antes"


def test_una_frontera_incoherente_se_descarta_en_vez_de_conciliarse() -> None:
    """Slug y `end_date` son las dos únicas fuentes: en desacuerdo, τ y K serían inventados."""
    assert epoch_from_market(market(), epoch_seconds=300.0) is None
    assert (
        epoch_from_market(market(state={"end_date": "2026-09-08T22:07:00Z"}), epoch_seconds=900.0)
        is None
    )


def test_un_slug_sin_sufijo_numerico_no_produce_epoca() -> None:
    assert epoch_from_market(market(slug="btc-updown-15m-preview"), epoch_seconds=900.0) is None


def test_tau_no_es_negativo_despues_del_cierre() -> None:
    epoch = epoch_from_market(market(), epoch_seconds=900.0)
    assert epoch is not None
    assert epoch.tau_s(datetime(2026, 9, 8, 21, 45, tzinfo=UTC)) == 900.0
    assert epoch.tau_s(datetime(2026, 9, 8, 23, 0, tzinfo=UTC)) == 0.0


def test_contains_marca_la_frontera_por_el_lado_correcto() -> None:
    """El instante de cierre pertenece a la época SIGUIENTE, no a la que vence."""
    epoch = epoch_from_market(market(), epoch_seconds=900.0)
    assert epoch is not None
    assert epoch.contains(datetime(2026, 9, 8, 21, 45, tzinfo=UTC))
    assert epoch.contains(datetime(2026, 9, 8, 21, 59, 59, tzinfo=UTC))
    assert not epoch.contains(datetime(2026, 9, 8, 22, 0, tzinfo=UTC))


def test_current_of_elige_la_epoca_que_contiene_el_instante() -> None:
    first = epoch_from_market(market(), epoch_seconds=900.0)
    second = epoch_from_market(
        market(
            slug=slug_for("btc-updown-15m", EPOCH_START + 900),
            state={"end_date": "2026-09-08T22:15:00Z"},
        ),
        epoch_seconds=900.0,
    )
    assert first is not None and second is not None
    epochs = [first, second]
    assert current_of(epochs, datetime(2026, 9, 8, 21, 50, tzinfo=UTC)) is first
    assert current_of(epochs, datetime(2026, 9, 8, 22, 5, tzinfo=UTC)) is second
    assert current_of(epochs, datetime(2026, 9, 8, 23, 0, tzinfo=UTC)) is None


# ---------------------------------------------------------------------------
# Desenlace: la variable dependiente del experimento de §8
# ---------------------------------------------------------------------------


def test_un_libro_moribundo_no_es_un_desenlace() -> None:
    """Medido: una época recién vencida daba `closed=False` con `Up=0.005`."""
    dying = market(
        state={"closed": False, "accepting_orders": True},
        outcomes={
            "yes": {"label": "Up", "token_id": "111", "price": "0.005"},
            "no": {"label": "Down", "token_id": "222", "price": "0.995"},
        },
    )
    assert resolved_outcome(dying) is None


def test_se_lee_el_desenlace_de_una_epoca_resuelta() -> None:
    resolved = market(
        state={"closed": True, "accepting_orders": False},
        outcomes={
            "yes": {"label": "Up", "token_id": "111", "price": "0"},
            "no": {"label": "Down", "token_id": "222", "price": "1"},
        },
    )
    assert resolved_outcome(resolved) == OUTCOME_DOWN

    up = market(
        state={"closed": True, "accepting_orders": False},
        outcomes={
            "yes": {"label": "Up", "token_id": "111", "price": "1"},
            "no": {"label": "Down", "token_id": "222", "price": "0"},
        },
    )
    assert resolved_outcome(up) == OUTCOME_UP


def test_uma_resuelto_tambien_vale_como_cierre() -> None:
    """El estado de UMA es la señal más directa; `closed` puede tardar en reflejarlo."""
    resolved = market(
        resolution={"uma_resolution_status": "resolved"},
        outcomes={
            "yes": {"label": "Up", "token_id": "111", "price": "1"},
            "no": {"label": "Down", "token_id": "222", "price": "0"},
        },
    )
    assert resolved_outcome(resolved) == OUTCOME_UP


def test_precios_no_complementarios_no_se_leen_como_desenlace() -> None:
    """Si los dos lados no suman 1, lo que se está leyendo no es una resolución."""
    weird = market(
        state={"closed": True},
        outcomes={
            "yes": {"label": "Up", "token_id": "111", "price": "1"},
            "no": {"label": "Down", "token_id": "222", "price": "1"},
        },
    )
    assert resolved_outcome(weird) is None


@pytest.mark.parametrize("price", ["0.4", "0.6", "0.5"])
def test_un_mercado_cerrado_sin_resolver_no_inventa_desenlace(price: str) -> None:
    complement = str(1 - float(price))
    ambiguous = market(
        state={"closed": True},
        outcomes={
            "yes": {"label": "Up", "token_id": "111", "price": price},
            "no": {"label": "Down", "token_id": "222", "price": complement},
        },
    )
    assert resolved_outcome(ambiguous) is None
