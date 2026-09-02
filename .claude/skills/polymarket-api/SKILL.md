---
name: polymarket-api
description: Consultar SIEMPRE antes de escribir, revisar o depurar cualquier código que llame a la API de Polymarket, el CLOB, RTDS, Gamma, o que use el SDK polymarket-client. Cubre endpoints, nombres de campo, formatos de suscripción, direcciones de contrato, fees, tick sizes y límites de rate. Dispara también al depurar un rechazo de orden o una respuesta inesperada.
---

# Verificación de la API de Polymarket

## Regla dura

CLOB V2 entró en producción el 28-abr-2026. **Todo lo anterior a mayo de 2026 en tu
memoria de entrenamiento está potencialmente obsoleto**, incluidos nombres de campo,
direcciones de contrato y firmas de método.

Antes de escribir código que toque la API:

1. `WebFetch https://docs.polymarket.com/llms.txt` para localizar la página correcta.
2. `WebFetch` la página `.md` concreta.
3. Cita en un comentario del código la URL y la fecha de consulta.

## Nunca

- No inventes nombres de campo. Si no los viste en la doc en esta sesión, dilo.
- No copies direcciones de contrato de blogs, repos o de memoria.
- No asumas que un método del SDK existe. Verifica la firma.
- Si la doc no responde, escribe un script que imprima la respuesta cruda y míralo,
  en vez de adivinar la forma.

## Páginas frecuentes

/getting-started/python.md · /market-data/chainlink-twap.md · /market-data/realtime-data.md
/trading/fees.md · /trading/place-orders.md · /api-reference/geoblock.md
/resources/contracts.md · /api-reference/trading-rate-limits.md
