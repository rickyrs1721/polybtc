"""Fila del grabador y su esquema Arrow. Es el contrato de datos de toda la Fase 0.

Dos decisiones que condicionan el análisis del Sprint 5 y por eso se justifican aquí:

**1. Los precios se guardan como `float64`, no como `decimal128`.**

§4 exige `Decimal` en `feeds/`, `execution/` y `risk/`, y ahí se cumple: lo que entra a este
módulo ya viene en `Decimal` desde el feed. Pero el Parquet es el sustrato de ANÁLISIS, y
una columna `decimal128` llega a pandas como objetos `Decimal` de Python: cualquier media,
resta o histograma sobre ella se vuelve aritmética de objetos, lenta y llena de sorpresas.

La exactitud que §4 protege es la del camino de ejecución —precio de una orden, tamaño,
saldo—, y ninguno de esos números pasa por aquí. Un precio de BTC de cinco cifras tiene 11
dígitos de margen en `float64`, y un bid de 0.01 es exacto a 1e-18. La conversión ocurre en
un solo sitio, `_f()`, igual que la del modelo ocurre solo en `model/fair.py::to_execution`.
Esta va en la dirección contraria y no alimenta ninguna orden: no es la misma frontera.

**2. El fair se graba bajo TODAS las hipótesis de ventana, no bajo una.**

La ventana TWAP del mercado es una decisión abierta de §8 y se decide con estos mismos
datos. Grabar una sola hipótesis obligaría a acertar antes de tener con qué acertar, y si
se falla los 14 días no valen nada. Las columnas se generan a partir de
`Settings.twap_window_hypotheses_s`, así que añadir o quitar una hipótesis no toca código.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal

import pyarrow as pa

TIMESTAMP_TYPE = pa.timestamp("us", tz="UTC")


def window_suffix(window_s: float) -> str:
    """`60.0 → "w60"`. Sufijo de columna de una hipótesis de ventana."""
    return f"w{window_s:g}"


@dataclass(frozen=True, slots=True)
class ModelPoint:
    """Salida del modelo bajo UNA hipótesis de ventana. Todos los insumos, que §9 exige."""

    fair: float
    fair_band: float
    band_low: float
    band_high: float
    spot_eff: float
    variance: float
    regime: str
    delta_per_dollar: float


@dataclass(frozen=True, slots=True)
class Sample:
    """Una fila: el estado completo del sistema en un instante.

    Todo es opcional salvo `ts` porque en la Fase 0 el estado incompleto es información:
    un `fair` nulo con `halt_reasons="basis_no_converge"` es exactamente lo que hay que
    poder contar después. Rellenar huecos con ceros los volvería invisibles.
    """

    ts: datetime

    chainlink: float | None = None
    twap30: float | None = None
    twap60: float | None = None
    binance_mid: float | None = None
    binance_relay: float | None = None

    basis_log: float | None = None
    basis_usd: float | None = None
    """El basis en DÓLARES. En logs el número es ilegible y nadie nota que se mueve."""
    basis_ready: bool = False
    basis_samples: int = 0
    basis_clamped: bool = False
    spot_oracle: float | None = None
    """`binance_mid` llevado a unidades del oráculo (§2.1.b). El insumo real del modelo."""

    sigma_s: float | None = None
    sigma_annual: float | None = None
    sigma_ready: bool = False
    sigma_in_band: bool = False
    sigma_samples: int = 0

    epoch_slug: str | None = None
    epoch_end: datetime | None = None
    tau_s: float | None = None
    strike: float | None = None
    strike_source: str | None = None
    """`open` = capturado en la apertura de la época; `missed` = arrancamos a mitad."""

    up_bid: float | None = None
    up_ask: float | None = None
    down_bid: float | None = None
    down_ask: float | None = None

    up_tick: float | None = None
    down_tick: float | None = None
    """Tick VIGENTE de cada libro, tal como lo dijo el servidor. Nunca uno asumido.

    Está aquí porque el tick **sigue al precio, no al mercado**: medido el 2026-09-11 sobre
    la misma familia y el mismo día, una época deep-OTM cotizaba con tick `0.001` y tres
    épocas en torno a `0.51` con tick `0.01`. Sin esta columna, un spread de medio céntimo
    en el Parquet es ambiguo —¿libro pegado al suelo en régimen fino, o libro estrecho en
    régimen grueso?— y la pregunta 1 de §6 no se puede responder.
    """

    models: dict[float, ModelPoint] = field(default_factory=dict)
    """Salida del modelo por hipótesis de ventana. Ver la decisión 2 del módulo."""

    halted: bool = False
    halt_reasons: str = ""
    """Banderas activas separadas por coma. Vacío = se podría cotizar (§5.2)."""

    age_chainlink: float | None = None
    age_twap30: float | None = None
    age_twap60: float | None = None
    age_binance_direct: float | None = None
    age_binance_relay: float | None = None
    age_book_up: float | None = None
    age_book_down: float | None = None
    reconnects: int = 0


_BASE_FIELDS: tuple[tuple[str, pa.DataType], ...] = (
    ("ts", TIMESTAMP_TYPE),
    ("chainlink", pa.float64()),
    ("twap30", pa.float64()),
    ("twap60", pa.float64()),
    ("binance_mid", pa.float64()),
    ("binance_relay", pa.float64()),
    ("basis_log", pa.float64()),
    ("basis_usd", pa.float64()),
    ("basis_ready", pa.bool_()),
    ("basis_samples", pa.int32()),
    ("basis_clamped", pa.bool_()),
    ("spot_oracle", pa.float64()),
    ("sigma_s", pa.float64()),
    ("sigma_annual", pa.float64()),
    ("sigma_ready", pa.bool_()),
    ("sigma_in_band", pa.bool_()),
    ("sigma_samples", pa.int32()),
    ("epoch_slug", pa.string()),
    ("epoch_end", TIMESTAMP_TYPE),
    ("tau_s", pa.float64()),
    ("strike", pa.float64()),
    ("strike_source", pa.string()),
    ("up_bid", pa.float64()),
    ("up_ask", pa.float64()),
    ("down_bid", pa.float64()),
    ("down_ask", pa.float64()),
    ("up_tick", pa.float64()),
    ("down_tick", pa.float64()),
)

_MODEL_FIELDS: tuple[tuple[str, pa.DataType], ...] = (
    ("fair", pa.float64()),
    ("fair_band", pa.float64()),
    ("band_low", pa.float64()),
    ("band_high", pa.float64()),
    ("spot_eff", pa.float64()),
    ("variance", pa.float64()),
    ("regime", pa.string()),
    ("delta_per_dollar", pa.float64()),
)

_TAIL_FIELDS: tuple[tuple[str, pa.DataType], ...] = (
    ("halted", pa.bool_()),
    ("halt_reasons", pa.string()),
    ("age_chainlink", pa.float64()),
    ("age_twap30", pa.float64()),
    ("age_twap60", pa.float64()),
    ("age_binance_direct", pa.float64()),
    ("age_binance_relay", pa.float64()),
    ("age_book_up", pa.float64()),
    ("age_book_down", pa.float64()),
    ("reconnects", pa.int32()),
)


def build_schema(windows: Sequence[float]) -> pa.Schema:
    """Esquema Arrow para las hipótesis de ventana dadas.

    El orden de `windows` fija el orden de las columnas, así que dos corridas con la misma
    configuración producen Parquet con el mismo esquema y `pyarrow.dataset` los lee juntos
    sin quejarse. Cambiar la configuración a mitad de la Fase 0 rompería esa unión — y es
    justo lo que el plan prohíbe cuando dice de no tocar parámetros mientras se mide.
    """
    fields = [pa.field(name, dtype) for name, dtype in _BASE_FIELDS]
    for window in windows:
        suffix = window_suffix(window)
        fields += [pa.field(f"{name}_{suffix}", dtype) for name, dtype in _MODEL_FIELDS]
    fields += [pa.field(name, dtype) for name, dtype in _TAIL_FIELDS]
    return pa.schema(fields)


def to_record_batch(samples: Sequence[Sample], windows: Sequence[float]) -> pa.RecordBatch:
    """Filas → un `RecordBatch` con el esquema de `build_schema`."""
    schema = build_schema(windows)
    columns: list[pa.Array] = []

    for name, dtype in _BASE_FIELDS:
        columns.append(pa.array([getattr(s, name) for s in samples], type=dtype))

    for window in windows:
        points = [s.models.get(window) for s in samples]
        for name, dtype in _MODEL_FIELDS:
            values = [None if p is None else getattr(p, name) for p in points]
            columns.append(pa.array(values, type=dtype))

    for name, dtype in _TAIL_FIELDS:
        columns.append(pa.array([getattr(s, name) for s in samples], type=dtype))

    return pa.RecordBatch.from_arrays(columns, schema=schema)


def f(value: Decimal | float | None) -> float | None:
    """`Decimal` del feed → `float` para el Parquet. Única conversión de este módulo.

    Ver la decisión 1 del docstring del módulo: esto NO es la frontera de §4, que va en el
    otro sentido y termina en una orden. De aquí no sale ningún precio hacia el CLOB.
    """
    return None if value is None else float(value)
