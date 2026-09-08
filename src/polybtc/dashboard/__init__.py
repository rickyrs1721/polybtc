"""Dashboard de la Fase 0. Proceso aparte, de SOLO LECTURA, y sin importar el bot.

No importa ningún módulo de ejecución ni comparte memoria con el grabador: lee `state.db`
con una conexión `mode=ro` y los Parquet ya cerrados. Si esto revienta, el grabador sigue
escribiendo; acoplarlos es cómo un error de renderizado se lleva 14 días de datos.

**No hay panel de PnL, y es deliberado.** En la Fase 0 no se opera. Un PnL a cero en
pantalla solo invita a saltarse la fase, así que el panel llega en el Sprint 6, con el
coste de infraestructura como línea fija (§5.3).
"""
