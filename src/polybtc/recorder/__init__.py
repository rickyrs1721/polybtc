"""Grabador de la Fase 0: una fila por segundo a Parquet, y el estado vivo a SQLite.

Dos procesos separados, y no es negociable: el grabador escribe, el dashboard solo lee. Si
el dashboard revienta, el grabador sigue. Acoplarlos es cómo un error de renderizado se
lleva por delante 14 días de datos (§6, Fase 0).

    grabador (escribe) → SQLite WAL (estado vivo) ← dashboard (solo lee)
                       → Parquet por hora        ↙ (análisis, Sprint 5)

Aquí no se opera nada. No hay PnL, no hay órdenes, no hay wallet: la Fase 0 es observación
pura y su único producto son datos con los que responder las seis preguntas de §6.
"""
