"""EWMA de log(chainlink/binance) con clamp: lleva el spot a unidades del oráculo (§2.1.b).

Binance cotiza BTC**USDT**; Chainlink agrega varias plazas en BTC/**USD**. Entre el
desanclaje de USDT y la prima por plaza hay una diferencia persistente de decenas de
dólares. Sin corregirla, ese basis entra íntegro en `ln(S/K)` y produce una señal falsa
constante y siempre en la misma dirección — el fallo que parece una estrategia rentable
durante dos días y luego devuelve todo con intereses.

El EWMA es multiplicativo (vive en logs). Cada observación se clampea para que un tick
corrupto no envenene el estimador, y hasta que hay muestras suficientes `ready` es False:
un basis mal estimado es peor que ninguno, así que la respuesta correcta mientras tanto
es NO COTIZAR (§5.2), no cotizar con el basis a medias.
"""

from __future__ import annotations

import math

DEFAULT_HALF_LIFE_S = 600.0
"""Half-life de ~10 minutos (§2.1.b)."""

DEFAULT_CLAMP = 0.01
"""±1% por observación: un tick corrupto no debe mover el estimador (§2.1.b)."""


class BasisEstimator:
    """EWMA en tiempo de `ln(chainlink / binance)`.

    El decaimiento es por tiempo transcurrido, no por número de observaciones: los dos
    feeds llegan a ritmos distintos e irregulares, y una half-life en "ticks" mediría el
    ritmo de mensajes en vez de los 10 minutos que se quieren.
    """

    def __init__(
        self,
        half_life_s: float = DEFAULT_HALF_LIFE_S,
        clamp: float = DEFAULT_CLAMP,
        min_samples: int = 30,
    ) -> None:
        if half_life_s <= 0.0:
            raise ValueError(f"half_life_s debe ser positiva: {half_life_s}")
        if clamp <= 0.0:
            raise ValueError(f"clamp debe ser positivo: {clamp}")
        if min_samples < 1:
            raise ValueError(f"min_samples debe ser ≥ 1: {min_samples}")

        self._half_life_s = half_life_s
        self._clamp = clamp
        self._min_samples = min_samples

        self._log_basis: float = 0.0
        self._samples: int = 0
        self._last_ts: float | None = None
        self._last_clamped: bool = False

    def update(self, ts: float, chainlink: float, binance: float) -> None:
        """Ingiere una observación simultánea de ambos feeds."""
        if chainlink <= 0.0 or binance <= 0.0:
            raise ValueError(
                f"precios deben ser positivos: chainlink={chainlink}, binance={binance}"
            )

        observation = math.log(chainlink / binance)
        clamped = max(-self._clamp, min(self._clamp, observation))
        self._last_clamped = clamped != observation

        if self._last_ts is None:
            self._log_basis = clamped
        else:
            elapsed = max(0.0, ts - self._last_ts)
            decay = 0.5 ** (elapsed / self._half_life_s)
            self._log_basis = decay * self._log_basis + (1.0 - decay) * clamped

        self._last_ts = ts
        self._samples += 1

    def to_oracle(self, binance_price: float) -> float:
        """Precio de Binance expresado en unidades del oráculo: `binance · exp(basis)`.

        Lanza si el estimador no ha convergido: sin basis fiable el ancla está rota y lo
        que corresponde es retirar cotizaciones (§5.2), no cotizar con un número inventado.
        """
        if binance_price <= 0.0:
            raise ValueError(f"binance_price debe ser positivo: {binance_price}")
        if not self.ready:
            raise RuntimeError(
                f"basis sin converger ({self._samples}/{self._min_samples} muestras): no cotizar"
            )
        return binance_price * math.exp(self._log_basis)

    @property
    def log_basis(self) -> float:
        return self._log_basis

    @property
    def basis_ratio(self) -> float:
        """`chainlink/binance` estimado. Para logs y para el gráfico de la Fase 0."""
        return math.exp(self._log_basis)

    @property
    def samples(self) -> int:
        return self._samples

    @property
    def ready(self) -> bool:
        return self._samples >= self._min_samples

    @property
    def last_observation_clamped(self) -> bool:
        """La última observación se salió del clamp → señal de halt para `risk/` (§5.2)."""
        return self._last_clamped
