"""Parseo compartido de mensajes de feed: JSON → `Decimal`, nunca pasando por `float`.

`json.loads(..., parse_float=Decimal)` es el detalle que hace que §4 se cumpla de verdad
en `feeds/`. Sin él, un `65000.5` del mensaje se convierte en un `float` y ya perdiste la
exactitud antes de haber tocado nada: el `Decimal` que construyas después solo hereda el
error. Los tres feeds parsean por aquí para que la regla no dependa de recordarla.
"""

from __future__ import annotations

import json
from decimal import Decimal, InvalidOperation
from typing import Any


def loads_decimal(raw: str) -> Any | None:
    """JSON con los números decimales como `Decimal`. `None` si no es JSON.

    Devuelve `None` en vez de lanzar porque los servidores mandan frames de control que no
    son JSON (`PONG`, por ejemplo) y eso no es un error que deba cortar la conexión.
    """
    text = raw.strip()
    if not text or text[0] not in "{[":
        return None
    try:
        return json.loads(text, parse_float=Decimal)
    except json.JSONDecodeError:
        return None


def as_decimal(value: object) -> Decimal | None:
    """`Decimal` a partir de un valor ya parseado, o `None` si no es un número utilizable.

    Acepta `str` porque varias APIs mandan los precios entrecomillados, e `int` porque un
    entero JSON no pasa por `parse_float`. Rechaza `float` por omisión: si aparece uno es
    que alguien parseó sin `loads_decimal` y la exactitud ya se perdió río arriba.
    """
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, str):
        try:
            return Decimal(value)
        except InvalidOperation:
            return None
    return None
