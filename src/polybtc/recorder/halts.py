"""Evaluación de las banderas de halt de §5.2. En la Fase 0 se GRABAN, no se aplican.

La diferencia importa. Aquí no se cotiza nada, así que un halt no retira ninguna orden:
es una etiqueta sobre la fila que dice «con estos datos no se habría podido cotizar». Sin
esa etiqueta, el análisis del Sprint 5 contaría como oportunidad cada instante en que el
basis no había convergido o el TWAP estaba stale — y la respuesta a la pregunta 2 de §6
saldría inflada precisamente por los momentos en que el sistema estaba ciego.

La aplicación real de estos límites vive en `risk/limits.py` y es del Sprint 7, cuando hay
órdenes que retirar. Este módulo es su definición observacional, no su sustituto.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from polybtc.feeds.state import FeedState

REASON_STALE = "stale"
REASON_BASIS_NOT_READY = "basis_sin_converger"
REASON_BASIS_CLAMPED = "basis_fuera_de_clamp"
REASON_SIGMA_NOT_READY = "sigma_sin_muestras"
REASON_SIGMA_OUT_OF_BAND = "sigma_fuera_de_banda"
REASON_NO_MARKET = "sin_mercado"
REASON_NO_STRIKE = "sin_strike"


@dataclass(frozen=True, slots=True)
class Halts:
    """Motivos activos. Vacío = con estos datos se podría cotizar."""

    reasons: tuple[str, ...]

    @property
    def halted(self) -> bool:
        return bool(self.reasons)

    def joined(self) -> str:
        return ",".join(self.reasons)


def evaluate(
    state: FeedState,
    required_feeds: Sequence[tuple[str, str]],
    *,
    basis_ready: bool,
    basis_clamped: bool,
    sigma_ready: bool,
    sigma_in_band: bool,
    has_market: bool,
    has_strike: bool,
) -> Halts:
    """Todas las banderas activas, en orden estable.

    Se devuelven TODAS y no solo la primera: en el análisis interesa saber si un instante
    estuvo ciego por una razón o por cuatro a la vez, y cortocircuitar al primer motivo
    borraría esa distinción para siempre.

    `required_feeds` son pares `(clave, etiqueta)`. La clave de un libro lleva el token id
    dentro —77 dígitos— y ponerla en el texto haría una bandera ilegible en pantalla y una
    columna irrepetible en el Parquet: el token cambia con cada época (§8.1), así que
    `stale:book:1544…` no se puede ni agrupar. La etiqueta sí.
    """
    reasons: list[str] = []
    reasons += [f"{REASON_STALE}:{label}" for key, label in required_feeds if state.is_stale(key)]
    if not basis_ready:
        reasons.append(REASON_BASIS_NOT_READY)
    if basis_clamped:
        reasons.append(REASON_BASIS_CLAMPED)
    if not sigma_ready:
        reasons.append(REASON_SIGMA_NOT_READY)
    if not sigma_in_band:
        reasons.append(REASON_SIGMA_OUT_OF_BAND)
    if not has_market:
        reasons.append(REASON_NO_MARKET)
    elif not has_strike:
        # Solo si HAY mercado: sin época, «sin strike» es redundante y ensuciaría el
        # recuento del hueco de ~2 min entre épocas (§8.1), que es un estado normal.
        reasons.append(REASON_NO_STRIKE)
    return Halts(tuple(reasons))
