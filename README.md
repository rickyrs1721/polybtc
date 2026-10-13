# polybtc

Un bot de market making para mercados binarios de BTC en Polymarket, construido hasta el
punto exacto en el que los datos dijeron que no siguiera.

**Resultado: no hay edge, por ninguna de las tres vías que podían darlo.** El modelo
predice los desenlaces peor que el propio precio de mercado, en todos los horizontes y
también con su volatilidad óptimamente recalibrada. Y cuando se midió la vía que no exige
predecir nada —cobrar por proveer liquidez— resultó costar tres veces lo que paga. El
proyecto se detuvo antes de arriesgar un dólar.

Eso es lo que este repositorio documenta: no una estrategia rentable, sino **el método que
sirvió para descubrir que no lo era, por €6 y en catorce días.**

---

## El problema

Polymarket lista mercados binarios de 15 minutos: *¿estará BTC por encima de su precio de
apertura al final de la época?* Liquidan contra un TWAP de Chainlink. La hipótesis inicial
era la habitual: construir un fair value propio a partir del spot y cotizar como maker
cuando el mercado se separe de él.

Tres cosas hacían la hipótesis plausible y una la mató. Las tres primeras resultaron
ciertas; la cuarta no estaba en el plan.

---

## Qué se construyó

| Componente | Qué hace |
| --- | --- |
| `model/` | Fair value de la binaria: varianza terminal por régimen de ventana TWAP, spot efectivo, N(d₂) promediado sobre una banda de incertidumbre de σ |
| `feeds/` | RTDS (TWAP de Chainlink), spot de Binance y Coinbase, libro del CLOB. Reconexión con backoff y edad por feed |
| `recorder/` | Una fila por segundo a Parquet. Dos procesos separados del dashboard |
| `dashboard/` | FastAPI + SSE, solo lectura, paneles atados a las preguntas del análisis |
| `deploy/` | Preflight, bootstrap idempotente, unidades systemd, respaldo, informe de soak |
| `research/` | Monte Carlo del modelo, informe de la fase de medición y la selección adversa de un maker |

~9.200 líneas de Python, 160 tests, `ruff` y `mypy --strict` limpios.

---

## Las decisiones que lo definen

**El modelo se escribió contra tests de propiedad, no contra valores esperados.** No se
testea «el precio es 0,5312»; se testea que la varianza del TWAP sea menor que la puntual,
que haya continuidad en `τ = w`, que el fair converja al indicador cuando `τ → 0`. Fue lo
que atrapó un error de fórmula heredado: una varianza que sobreestimaba hasta un **98,8%**
y hacía colapsar el fair a certeza absoluta a diez segundos del cierre. Verificado contra
Monte Carlo con 300.000 trayectorias.

**El grabador no escribe Parquet de forma incremental.** Un Parquet sin footer no es un
archivo incompleto: es uno ilegible que además rompe la lectura del directorio entero. Cada
volcado va a un stream Arrow IPC con `fsync` y el Parquet se escribe de una vez al cerrar
la hora, con `rename` atómico. Un apagón cuesta un minuto, nunca una hora.

**Las decisiones abiertas se grabaron por duplicado en vez de elegirse.** La documentación
del mercado se contradecía sobre la ventana de liquidación —el título dice 15 minutos, la
fuente citada es un stream de 60 segundos— y entre ambas hipótesis hay un factor 15 en
sensibilidad. En lugar de apostar, el grabador escribió el fair bajo **las dos** junto a la
frontera y el desenlace de cada época. Catorce días después, los datos decidieron.

**El despliegue es un script versionado, no un agente con acceso remoto.** Lo que hace falta
cuando el servidor muere el día 9 de 14 no es poder entrar a arreglarlo: es poder rehacerlo
desde cero en cinco minutos, sobre otro proveedor si toca.

---

## Lo que los datos dijeron

**14 días · 1,6 millones de filas a 1 Hz · cero huecos · 1.779 épocas con desenlace real.**

### La ventana de liquidación es de 60 segundos

Contrastando cada hipótesis contra los desenlaces reales:

| Hipótesis | Acierto |
| --- | --- |
| **TWAP de 60 s** | **95,96%** |
| TWAP de 30 s | 95,76% |
| Precio puntual | 94,78% |
| Media del rango de 15 min *(lo que dice el título)* | 82,27% |

El mercado liquida contra la fuente que cita la página, no contra lo que promete su título.
Esa pregunta no la responde ninguna documentación pública.

### Y no hay edge

La métrica obvia parecía prometedora: `|fair − mid|` superaba los 3¢ el **62,6%** del
tiempo. Habría bastado para justificar seguir.

Es la trampa central del problema. **`|fair − mid|` no distingue «el mercado se equivoca»
de «yo me equivoco».** Mide las dos cosas sumadas.

Lo que sí distingue es contrastar ambas predicciones contra los desenlaces grabados:

| Brier *(menor es mejor)* | τ≤30s | τ≤300s | τ≤900s | **Total** |
| --- | --- | --- | --- | --- |
| **Mercado** | **0,0041** | **0,0997** | **0,2117** | **0,1474** |
| Modelo | 0,0387 | 0,1124 | 0,2199 | 0,1598 |
| Trivial (0,50) | 0,2500 | 0,2500 | 0,2500 | 0,2500 |

El modelo bate al 0,50 trivial —tiene información real— pero **el mercado tiene más, en
todos los tramos**. Y para descartar que fuera un problema de calibración, se barrió un
factor multiplicativo sobre σ y se le concedió al modelo su mejor caso posible:

| σ × | vs mercado |
| --- | --- |
| 1,00 | −8,29% |
| **1,50 (óptimo)** | **−6,37%** |
| 2,00 | −7,73% |

σ estaba subestimada un 50%. Corregirla no alcanza.

El informe completo, con las seis preguntas de la fase de medición, está en
[`docs/fase0-informe.md`](docs/fase0-informe.md) y lo reproduce
[`research/fase0_report.py`](src/polybtc/research/fase0_report.py).

### Y tampoco paga proveer liquidez

Perder la apuesta de predicción no cierra la pregunta entera: un maker cobra por estar ahí,
no por acertar. Esa vía se midió sobre los mismos datos, sin gastar nada más, con el umbral
escrito antes: *si la selección adversa a 60 s supera medio tick (0,5¢), se para*.

| Por share ejecutada | Céntimos |
| --- | --- |
| Deriva del mid en tu contra a 60 s | **2,265** |
| Neto del fill *(medio spread − deriva)* | −1,756 |
| Maker rebate del 20% | +0,265 |
| **Total** | **−1,491** |

**El precio no vuelve**: lo que en el instante del fill son 2,09¢ sigue siendo 2,27¢ un
minuto después. No es impacto transitorio, es información que se queda en el precio.

Y lo que convierte esa cifra en una conclusión es el control: la misma deriva medida en el
**millón de segundos en los que no hubo fill** da **+0,020¢**. Factor 113. El coste lo crea
la ejecución, no una tendencia del mercado — un test que no puede salir negativo no prueba
nada.

Hasta el sesgo del método se dejó como número comprobable en vez de como advertencia: el
estimador solo ve los fills causados por un movimiento de precio, así que esto solo sería
viable si **menos del 34,3%** de los fills reales fueran de ese tipo. Detalle en
[`docs/fase0b-informe.md`](docs/fase0b-informe.md).

---

## Lo que salió mal por el camino

Cada uno de estos fallaba **en silencio** — sin excepción, sin error, solo con un resultado
equivocado. Están documentados en el código con el número que los delató.

| Hallazgo | Síntoma |
| --- | --- |
| El campo `filters` de la suscripción apaga el stream | La suscripción se acusa y no llega un solo dato. Indistinguible de un problema de red |
| Buscar el mercado por fecha de vencimiento no lo encuentra | La primera página son 100 elementos y otras familias la llenan. El mercado existía y la búsqueda devolvía cero |
| `state.start_date` no es la apertura de la época | Es la creación del mercado, un día antes. Habría puesto el strike a un día del que liquida |
| El tick size sigue al precio, no al mercado | 0,001 cerca de los extremos y 0,01 en el medio, el mismo día. El CLOB rechaza órdenes con el tick equivocado |
| Binance no publica registros AAAA | En una máquina IPv6-only el grabador arranca, todo parece bien, y el spot no se actualiza jamás |
| El relevo de época bloqueaba el bucle de 1 Hz | 11 segundos perdidos cada cuarto de hora: 17 minutos de datos al día |
| `ProtectHome` convierte «no existe» en «permiso denegado» | Dos servicios en bucle de reinicio por un archivo de secretos que no debía existir |

Y uno metodológico, que vale más que los técnicos: **cambié de opinión tres veces sobre qué
fuente de spot usar**, con datos delante cada vez. El vaivén no venía de muestras cortas
sino de medir la dispersión en crudo en lugar del residuo tras la corrección. Más datos no
arreglan una métrica mal elegida.

---

## Qué demuestra este repositorio

No que haya una estrategia rentable. Que **se puede llegar a un "no" fundamentado antes de
arriesgar capital**, y que eso es más barato que el "sí" que no era.

- Un modelo cuantitativo verificado contra simulación, no contra intuición.
- Decisiones abiertas tratadas como experimentos con su variable dependiente grabada.
- Infraestructura que corrió 18 días seguidos sin perder un segundo.
- Separación de procesos tal que un fallo del panel no puede tocar los datos.
- La disciplina de escribir los umbrales **antes** de mirar los resultados.
- Mediciones con grupo de control, para distinguir un hallazgo de un artefacto.
- Y la honestidad de aceptar el resultado cuando la métrica fácil decía lo contrario.

**Coste total: ~€6 de VPS. Capital arriesgado: $0.**

---

## Cómo leerlo

| Archivo | Qué contiene |
| --- | --- |
| [`CLAUDE.md`](CLAUDE.md) | La fuente de verdad: modelo, arquitectura, riesgo, y un registro de cambios donde cada corrección cita el número que invalidó a la anterior |
| [`docs/fase0-informe.md`](docs/fase0-informe.md) | El informe de la fase de medición y su veredicto |
| [`docs/fase0b-informe.md`](docs/fase0b-informe.md) | La pregunta del maker: qué cuesta que te llenen |
| [`deploy/README.md`](deploy/README.md) | El runbook de despliegue por fases |

```bash
uv sync
uv run pytest
uv run python -m polybtc.research.fase0_report      --data-dir <copia-de-datos>
uv run python -m polybtc.research.adverse_selection --data-dir <copia-de-datos>
```

---

*No es asesoramiento financiero ni una recomendación de inversión. Es el registro de un
experimento que terminó en "no", que era el desenlace más probable desde el principio.*
