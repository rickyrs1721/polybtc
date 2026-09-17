"""Listas de restricción geográfica de Polymarket, y qué significan para ESTE bot.

Consultado en `https://docs.polymarket.com/api-reference/geoblock.md` el **2026-09-17**.
Se copian aquí porque deciden dónde puede vivir la máquina, y una lista de memoria o de un
blog es exactamente lo que §1 prohíbe. Vuelve a la fuente antes del Sprint 7.

**La distinción que importa y que se pasa por alto fácilmente**: hay dos listas de
«close-only» y no son lo mismo.

- `CLOSE_ONLY_API` — restringido en el frontend **y en la API**. Desde ahí el bot **no puede
  abrir posiciones**, solo cerrarlas. Es una pared para el Sprint 7. Incluye **Francia y
  Alemania**, que es donde están los datacenters europeos baratos de casi todo el mundo.
- `CLOSE_ONLY_FRONTEND` — restringido solo en el frontend; **la API queda libre**. Un bot
  que habla con el CLOB funciona. Son Irlanda, Países Bajos, Japón, Corea del Sur y Malta.
- Lo que no aparece en ninguna lista no tiene restricción. En Europa: Finlandia, España,
  Lituania, Portugal, Austria, Chequia, Suecia, Dinamarca, Estonia…

Nada de esto afecta a la Fase 0: grabar no es operar, y RTDS, Gamma y Binance son públicos.
Decide dónde montar la máquina **definitiva**, porque mudarse entre el Sprint 4 y el 7
cambia el perfil de latencia con el que se midieron 14 días.
"""

from __future__ import annotations

from enum import StrEnum

GEOBLOCK_URL = "https://polymarket.com/api/geoblock"
"""Devuelve `{"blocked": bool, "ip": str, "country": str, "region": str}`.

`blocked` refleja la política del **frontend**, así que sale `true` en sitios desde los que
la API sí funciona. No se le hace caso: se enruta por `country` contra las listas de abajo.
"""

BLOCKED: frozenset[str] = frozenset({"IR", "SY", "CU", "KP", "UA-43", "UA-14", "UA-09"})
"""Sancionados por OFAC: bloqueo total."""

CLOSE_ONLY_API: frozenset[str] = frozenset(
    {
        "AU",
        "BY",
        "BE",
        "BI",
        "BR",
        "CA-BC",
        "CA-ON",
        "CA-AB",
        "CA-QC",
        "CF",
        "CD",
        "ET",
        "FR",
        "DE",
        "IQ",
        "IT",
        "LB",
        "LY",
        "MM",
        "NZ",
        "NI",
        "KP",
        "PL",
        "RU",
        "SG",
        "SO",
        "SK",
        "SS",
        "SD",
        "TW",
        "TH",
        "GB",
        "US",
        "UM",
        "VE",
        "YE",
        "ZW",
    }
)
"""Close-only en frontend **y API**. Desde aquí el bot no abre posiciones."""

CLOSE_ONLY_FRONTEND: frozenset[str] = frozenset({"IE", "JP", "MT", "NL", "KR"})
"""Close-only solo en el frontend. La API queda libre: el bot funciona."""


class Standing(StrEnum):
    """Qué puede hacer el bot desde un país."""

    BLOCKED = "bloqueado"
    CLOSE_ONLY_API = "close-only en la API"
    CLOSE_ONLY_FRONTEND = "close-only solo en el frontend"
    UNRESTRICTED = "sin restricción"

    @property
    def can_open_positions(self) -> bool:
        """¿Se pueden ABRIR posiciones? Es lo único que decide si sirve para el Sprint 7."""
        return self in (Standing.CLOSE_ONLY_FRONTEND, Standing.UNRESTRICTED)

    @property
    def can_record(self) -> bool:
        """La Fase 0 solo lee feeds públicos: sirve cualquier sitio que no esté bloqueado."""
        return self is not Standing.BLOCKED


def standing_for(country: str, region: str = "") -> Standing:
    """Clasifica un país (y su región, para Canadá y Ucrania) contra las tres listas."""
    code = country.strip().upper()
    subdivision = f"{code}-{region.strip().upper()}" if region.strip() else ""

    for candidate in (subdivision, code):
        if not candidate:
            continue
        if candidate in BLOCKED:
            return Standing.BLOCKED
        if candidate in CLOSE_ONLY_API:
            return Standing.CLOSE_ONLY_API
        if candidate in CLOSE_ONLY_FRONTEND:
            return Standing.CLOSE_ONLY_FRONTEND
    return Standing.UNRESTRICTED
