"""EWMA de σ por segundo sobre rejilla de 1 s, con piso/techo y banda de incertidumbre (§3.4).

Tres decisiones del documento que están cableadas aquí, y por qué:

- **Rejilla fija de 1 s, no tick a tick.** Los ticks de Binance llegan a ritmo irregular;
  estimando sobre ticks, σ acaba midiendo el volumen de mensajes y no el mercado.
- **Mid del top of book, no último trade.** El último trade mete bid-ask bounce, que el
  estimador interpreta como volatilidad real e infla σ de forma sistemática. Esta clase
  no puede verificar qué le pasan: `update()` espera el mid.
- **Piso y techo no recortan el valor.** `sigma_s` devuelve lo estimado e `in_band` dice
  si está dentro. Fuera de banda la respuesta es RETIRAR cotizaciones (§3.4), no cotizar
  más ancho — y para decidirlo hay que ver el número real, no uno recortado.
"""

from __future__ import annotations

import math

SECONDS_PER_YEAR = 365 * 24 * 3600
_SQRT_SECONDS_PER_YEAR = math.sqrt(SECONDS_PER_YEAR)

GRID_SECONDS = 1.0
"""Paso de la rejilla temporal sobre la que se muestrea el mid (§3.4)."""


def annualize(sigma_s: float) -> float:
    """σ por segundo → σ anual. SOLO para mostrar a un humano o loguear (§3.0)."""
    return sigma_s * _SQRT_SECONDS_PER_YEAR


def deannualize(sigma_annual: float) -> float:
    """σ anual → σ por segundo, que es la única unidad con la que se calcula (§3.0)."""
    return sigma_annual / _SQRT_SECONDS_PER_YEAR


class EwmaVol:
    """EWMA de la varianza de log-retornos del mid, muestreado en rejilla de 1 s.

    El valor de la rejilla en el segundo `k` es el último mid observado en o antes de `k`.
    Ante un hueco de datos mayor que un segundo NO se fabrican retornos planos (sesgarían
    σ a la baja justo tras una desconexión): se re-ancla la rejilla y se conserva la
    memoria acumulada del EWMA.
    """

    def __init__(
        self,
        half_life_s: float,
        floor_s: float,
        ceiling_s: float,
        min_samples: int = 60,
    ) -> None:
        if half_life_s <= 0.0:
            raise ValueError(f"half_life_s debe ser positiva: {half_life_s}")
        if floor_s < 0.0 or ceiling_s <= floor_s:
            raise ValueError(f"banda de sigma invalida: [{floor_s}, {ceiling_s}]")
        if min_samples < 1:
            raise ValueError(f"min_samples debe ser >= 1: {min_samples}")

        self._half_life_s = half_life_s
        self._floor_s = floor_s
        self._ceiling_s = ceiling_s
        self._min_samples = min_samples

        # Decaimiento por paso de rejilla (1 s), derivado de la half-life.
        self._decay = 0.5 ** (GRID_SECONDS / half_life_s)

        self._variance: float = 0.0
        self._samples: int = 0
        self._grid_second: int | None = None
        self._grid_mid: float = 0.0
        self._last_mid: float = 0.0

    def update(self, ts: float, mid: float) -> None:
        """Ingiere un tick. `ts` en segundos (epoch o monótono, pero consistente)."""
        if mid <= 0.0:
            raise ValueError(f"el mid debe ser positivo: {mid}")

        current_second = math.floor(ts)

        if self._grid_second is None:
            self._grid_second = current_second
            self._grid_mid = mid
            self._last_mid = mid
            return

        gap = current_second - self._grid_second
        if gap > 1:
            # Hueco de datos: re-anclar sin inventar retornos. La memoria del EWMA sigue.
            self._grid_second = current_second
            self._grid_mid = mid
            self._last_mid = mid
            return

        if gap == 1:
            sample = self._last_mid
            self._ingest(math.log(sample / self._grid_mid))
            self._grid_mid = sample
            self._grid_second = current_second

        self._last_mid = mid

    def _ingest(self, log_return: float) -> None:
        squared = log_return * log_return
        # El primer retorno siembra el estimador: arrancar en 0 arrastraría un sesgo a la
        # baja durante varias half-lives, y `ready` no lo distinguiría de una vol real.
        self._variance = (
            squared
            if self._samples == 0
            else self._decay * self._variance + (1.0 - self._decay) * squared
        )
        self._samples += 1

    @property
    def sigma_s(self) -> float:
        """σ por segundo estimada, SIN recortar por la banda."""
        return math.sqrt(self._variance)

    @property
    def sigma_annual(self) -> float:
        """σ anualizada. Solo para logs (§3.0)."""
        return annualize(self.sigma_s)

    @property
    def samples(self) -> int:
        return self._samples

    @property
    def ready(self) -> bool:
        """¿Hay muestras suficientes para creerle al estimador?"""
        return self._samples >= self._min_samples

    @property
    def in_band(self) -> bool:
        """¿σ está dentro del piso/techo duros? Si no, la acción es retirar (§3.4)."""
        return self._floor_s <= self.sigma_s <= self._ceiling_s
