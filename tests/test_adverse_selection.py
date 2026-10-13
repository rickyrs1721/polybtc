"""El detector de fills de la Fase 0.b, contra libros construidos a mano.

El veredicto de la Fase 0.b —2,265¢ contra un umbral de 0,5¢— descansa entero en `scan()`.
Un error de signo ahí invertiría la conclusión sin dar ningún síntoma, así que estos tests
fijan los casos donde el resultado se puede calcular a mano:

- un libro que no se mueve no produce ni un fill, y su control es exactamente cero;
- cuando el ask cruza nuestro bid, la deriva sale positiva (en nuestra contra) y el neto
  negativo, con los valores exactos del movimiento;
- el lado vendedor es el espejo del comprador;
- un horizonte que cae en la época siguiente devuelve `None`, no un número de otro mercado.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pyarrow as pa

from polybtc.research.adverse_selection import HORIZONS, rebate_cents, scan

BASE = datetime(2026, 10, 1, 12, 0, 0, tzinfo=UTC)


def table_of(books: list[tuple[float, float]], *, epoch_at: int | None = None) -> pa.Table:
    """Una serie a 1 Hz con el libro dado. `epoch_at` parte la época en ese segundo."""
    rows = len(books)
    return pa.table(
        {
            "ts": pa.array(
                [BASE + timedelta(seconds=i) for i in range(rows)],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "epoch_slug": ["e1" if epoch_at is None or i < epoch_at else "e2" for i in range(rows)],
            "tau_s": [float(rows - i) for i in range(rows)],
            "up_bid": [b for b, _ in books],
            "up_ask": [a for _, a in books],
            "up_tick": [0.01] * rows,
        }
    )


def index_of(horizon: int) -> int:
    return HORIZONS.index(horizon)


def test_libro_quieto_no_produce_fills_y_su_control_es_cero() -> None:
    fills, control = scan(table_of([(0.50, 0.51)] * 200), "up")
    assert fills == []
    assert control  # el control sí se puebla: son los segundos sin fill
    assert max(abs(c) for c in control) < 1e-9


def test_el_ask_que_cruza_nuestro_bid_cuesta_lo_que_se_movio() -> None:
    # Un tick abajo en el segundo 100 y ahí se queda: mid 0.505 -> 0.495.
    books = [(0.50, 0.51)] * 100 + [(0.49, 0.50)] * 100
    fills, _ = scan(table_of(books), "up")

    assert len(fills) == 1
    fill = fills[0]
    assert fill.side == "buy"
    assert fill.price == 0.50
    for horizon in HORIZONS:
        # Deriva: el mid se fue 1c en nuestra contra y no vuelve. Positivo = en contra.
        assert abs((fill.drift_cents[index_of(horizon)] or 0.0) - 1.0) < 1e-9
        # Neto: compramos a 0.50 y el mid quedó en 0.495. Medio spread menos deriva.
        assert abs((fill.net_cents[index_of(horizon)] or 0.0) + 0.5) < 1e-9


def test_el_lado_vendedor_es_el_espejo() -> None:
    books = [(0.50, 0.51)] * 100 + [(0.51, 0.52)] * 100
    fills, _ = scan(table_of(books), "up")

    assert len(fills) == 1
    fill = fills[0]
    assert fill.side == "sell"
    assert fill.price == 0.51
    assert abs((fill.drift_cents[index_of(60)] or 0.0) - 1.0) < 1e-9
    assert abs((fill.net_cents[index_of(60)] or 0.0) + 0.5) < 1e-9


def test_un_horizonte_en_la_epoca_siguiente_no_devuelve_numero() -> None:
    books = [(0.50, 0.51)] * 100 + [(0.49, 0.50)] * 100
    fills, _ = scan(table_of(books, epoch_at=130), "up")

    assert len(fills) == 1
    fill = fills[0]
    assert fill.drift_cents[index_of(0)] is not None  # dentro de la época
    assert fill.drift_cents[index_of(10)] is not None
    assert fill.drift_cents[index_of(60)] is None  # el segundo 160 ya es otro mercado


def test_un_hueco_en_la_grabacion_invalida_el_horizonte() -> None:
    """Sin esto, un salto de reloj convertiría «60 s después» en un horizonte cualquiera."""
    books = [(0.50, 0.51)] * 100 + [(0.49, 0.50)] * 100
    table = table_of(books)
    stamps = table.column("ts").to_pylist()
    hole = [t if i < 150 else t + timedelta(seconds=600) for i, t in enumerate(stamps)]
    table = table.set_column(
        table.schema.get_field_index("ts"),
        "ts",
        pa.array(hole, type=pa.timestamp("us", tz="UTC")),
    )

    fills, _ = scan(table, "up")
    assert len(fills) == 1
    assert fills[0].drift_cents[index_of(30)] is not None  # el segundo 130, intacto
    assert fills[0].drift_cents[index_of(60)] is None  # el 160 cayó tras el hueco


def test_el_rebate_sale_de_la_formula_de_la_fee() -> None:
    # 0.2 x 0.07 x p x (1-p), en céntimos. A p=0.50: 0.35c por share.
    assert abs(rebate_cents(0.50) - 0.35) < 1e-9
    assert rebate_cents(0.95) < rebate_cents(0.50)
    assert abs(rebate_cents(0.30) - rebate_cents(0.70)) < 1e-9
