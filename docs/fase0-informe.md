# Informe de la Fase 0 — veredicto: **parar**

**Ventana:** 25-sep 04:20 → 11-oct 04:00 UTC · 1.381.162 filas a 1 Hz · 1.537 épocas con
desenlace real · cero huecos.

Los números los genera `src/polybtc/research/fase0_report.py`; este documento es la
lectura. La separación es deliberada: las cifras se pueden volver a producir, la
interpretación se discute.

---

## El veredicto, primero

**No hay edge. El proyecto no avanza al Sprint 6.**

El modelo predice los desenlaces **peor que el precio de mercado**, en todos los tramos de
`τ`, y sigue perdiendo incluso con σ recalibrada a su mejor valor posible. Lo que parecía
una discrepancia explotable era error propio.

| Brier (menor es mejor) | τ≤30s | τ≤60s | τ≤300s | τ≤900s | **TOTAL** |
| --- | --- | --- | --- | --- | --- |
| **Mercado (mid)** | **0,0041** | **0,0173** | **0,0997** | **0,2117** | **0,1474** |
| Modelo w60 | 0,0387 | 0,0413 | 0,1124 | 0,2199 | 0,1598 |
| Modelo w900 | 0,1765 | 0,1749 | 0,1800 | 0,2393 | 0,2082 |
| Trivial (0,50) | 0,2500 | 0,2500 | 0,2500 | 0,2500 | 0,2500 |

El modelo bate al 0,50 trivial —tiene información real— pero **el mercado tiene más**, y
por un margen que no se cierra con calibración:

| σ × | Brier | vs mercado |
| --- | --- | --- |
| 1,00 (la que teníamos) | 0,1597 | −8,29% |
| **1,50 (la óptima)** | **0,1568** | **−6,37%** |
| 2,00 | 0,1588 | −7,73% |

Ese barrido es el punto que hace el veredicto sólido. Sin él, la objeción obvia sería «el
modelo pierde porque su σ estaba mal». Se le concede al modelo su mejor caso posible —el
factor 1,5 confirma además que σ estaba subestimada en un 50%— y **aun así pierde un
6,37%**.

---

## Las seis preguntas de §6

### 1. Distribución del spread

p50 = **1,00¢** · p90 = **1,00¢** · máx 74¢ · **92,7% del tiempo pegado al tick**.

El libro está casi siempre al mínimo posible. Para una estrategia maker-only (§2.2) eso
significa que **no hay sitio donde ponerse dentro del spread**: solo cabe unirse al toque y
hacer cola. El techo de lo capturable es medio tick.

### 2. ¿Con qué frecuencia `|fair − mid|` supera 1,75¢ y 3¢?

| | >1,75¢ | >3¢ | media |
| --- | --- | --- | --- |
| w60 | 73,0% | **62,6%** | 6,74¢ |
| w900 | 82,9% | 78,7% | 16,43¢ |

**Y aquí está la trampa de toda la fase.** Leída sola, esta pregunta parece decir «hay
oportunidad el 62% del tiempo». No lo dice: `|fair − mid|` mide la discrepancia entre el
modelo y el mercado, y esa discrepancia puede venir de que el mercado se equivoque **o de
que me equivoque yo**. No distingue. El Brier de arriba sí, y dice lo segundo.

La tabla de go/no-go de §6 se construyó alrededor de esta pregunta y por eso resulta
inaplicable tal cual: su fila 1 («<1% → parar») y su fila 3 («edge presente → seguir»)
asumen un modelo calibrado. Es una lección sobre la propia tabla, no un fallo de los datos.

### 3. ¿Cuánto rezaga el TWAP al spot? ¿Es predecible?

`twap30` ≈ spot retrasado **18 s** (corr 0,99999) · `twap60` ≈ spot retrasado **32 s**
(corr 0,99997).

Exactamente lo que debe hacer una media móvil: la mitad de su ventana. **Es predecible casi
perfectamente**, lo que valida la maquinaria de `S_eff` de §3.3. Esta pieza del modelo está
bien; el problema no está aquí.

### 4. ¿Cuánto sobrevive una orden en el book?

El nivel del toque aguanta **p50 = 2 s**, p90 = 8 s, media 3,9 s, en ambos lados.

Dos segundos es el horizonte de «el mercado tal como lo viste». Junto con la pregunta 1
—sin sitio dentro del spread— dibuja un entorno hostil para un maker lento: te pasan por
delante constantemente y la cola se reordena antes de que tu orden madure.

### 5. El basis y su estabilidad — la fila 0 del go/no-go

Lo que importa no es la dispersión en crudo sino **el residuo después de la corrección
EWMA**, que es lo único que el modelo no puede quitar:

| | media | desviación | p95 | a τ=300 s | a τ=60 s |
| --- | --- | --- | --- | --- | --- |
| **binance** | −$0,03 | **$6,51** | $12,48 | **1,77¢** | 6,40¢ |
| coinbase | +$0,01 | $8,21 | $16,59 | 2,24¢ | 8,06¢ |

**La fila 0 NO se dispara.** El estimador converge, el residuo no tiene sesgo (media ~$0) y
a τ=300 s se queda en 1,77¢, por debajo del colchón de 3¢. A τ=60 s lo supera — que es
literalmente lo que §2.1.c predijo: *«el residuo se queda por debajo del colchón salvo
cerca del vencimiento»*. El ancla aguanta.

Y **binance gana a coinbase** sobre la métrica correcta, al revés que sobre la dispersión
en crudo. Ver la nota sobre los tres cambios de opinión más abajo.

### 6. ¿En qué franja de τ está la discrepancia?

| τ | 0-30s | 30-60s | 60-120s | 120-300s | 300-600s | 600-900s |
| --- | --- | --- | --- | --- | --- | --- |
| % >3¢ (w60) | 7% | 17% | 32% | 55% | **74%** | 71% |

Crece con τ, que es **lo contrario** de lo que haría un edge de latencia. Y el Brier por
tramo explica por qué: a τ pequeño el mercado es casi perfecto (0,0041) porque la ventana
TWAP ya está medio determinada, mientras que el modelo sigue dudando. La discrepancia
grande a τ alto es el modelo siendo más extremo que un mercado que acierta más.

---

## §8 cerrada: la ventana TWAP es de **60 segundos**

Contra 1.534 épocas con desenlace real, prediciendo con `>=` (el empate resuelve Up):

| Hipótesis | Acierto | Fallos |
| --- | --- | --- |
| **w60 — TWAP de 60 s en T** | **95,96%** | 62 |
| w30 — TWAP de 30 s en T | 95,76% | 65 |
| puntual — chainlink en T | 94,78% | 80 |
| **w900 — media del rango del título** | **82,27%** | 272 |

El mercado liquida contra **el stream `btc-usd-twap-60s` que cita la propia página**, no
contra el «TWAP del rango de 15 minutos» que sugiere el título. La contradicción del
producto se resuelve a favor de la fuente citada, y por un margen de 14 puntos.

El 4% restante de fallos es esperable: nuestro `settle_twap60` se captura en el tick más
cercano a T y Chainlink no publica los detalles de muestreo de su TWAP (§3.2 ya decía que
no se intentara reproducir).

**Esta decisión era la única abierta que bloqueaba el modelo, y ahora está cerrada con
datos.** Tiene valor aunque el proyecto no siga: es la respuesta a la pregunta que ningún
documento de Polymarket contesta.

---

## Lo que costó aprender, y conviene no repetir

**Cambié de opinión tres veces sobre qué spot usar**, y las tres con datos delante:

| Momento | Métrica usada | Veredicto |
| --- | --- | --- |
| Antes de medir | latencia al libro + denominación USD | coinbase |
| 72 h | dispersión en crudo | binance |
| 289 h | dispersión en crudo | coinbase |
| **14 días** | **residuo tras EWMA** | **binance** |

El vaivén no vino de muestras cortas sino de **usar la métrica equivocada**. La dispersión
en crudo incluye la deriva lenta del nivel, que el estimador de §2.1.b sigue sin problema;
lo único que envenena `ln(S/K)` es lo que queda después de restarla. En cuanto se mide eso,
la respuesta es estable y es binance.

La lección general: **más datos no arreglan una métrica mal elegida.**

---

## Lo que queda construido

Independientemente del veredicto:

- Un modelo de fair value para binarias liquidadas por TWAP, con su varianza por régimen
  verificada por Monte Carlo, y 154 tests.
- Un grabador que corrió **16 días sin perder un segundo**, con escritura a prueba de
  apagones y despliegue reproducible en cinco minutos.
- Un dashboard de solo lectura en proceso aparte.
- Una metodología que distinguió «hay oportunidad» de «mi modelo está mal» antes de
  arriesgar un dólar.
- Coste total: **~€6 de VPS**. Capital en riesgo: **$0**.
