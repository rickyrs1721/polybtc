"""Despliegue de la Fase 0: un script versionado, no un agente con SSH.

Lo que hace falta cuando el VPS se muere el día 9 de los 14 no es poder entrar a
arreglarlo: es poder **rehacerlo desde cero en cinco minutos**, sobre otro proveedor si
toca. Eso lo da un script idempotente en el repositorio; no lo da un servidor MCP, un flujo
de N8N ni un agente con acceso remoto. §1 del plan ya decidió que este repo no añade
superficie de ataque, y la máquina que en el Sprint 7 guardará una llave privada es el peor
sitio posible para empezar a hacerlo.

Efecto secundario que vale tanto como el principal: cambiar de proveedor cuesta una tarde,
así que la elección del VPS deja de ser una decisión difícil de revertir.

Se ejecuta como módulo para que los imports entre estos archivos funcionen igual dentro y
fuera del repo:

    uv run python -m deploy.preflight
    uv run python -m deploy.soak_report --data-dir /var/lib/polybtc/data
"""
