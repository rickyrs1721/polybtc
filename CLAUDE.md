# CLAUDE.md — polybtc

Bot de trading sobre mercados cripto de corto plazo de Polymarket.
Este archivo es la fuente de verdad del proyecto. Léelo completo antes de escribir código.

> **v2 — agosto 2026.** Corrige la fórmula de varianza TWAP (era incorrecta y mezclaba
> unidades), añade la corrección de basis, ajusta la regla de `Decimal` y replantea el
> sizing. El registro de cambios está en §10. Si vienes de la v1, lee §3 entera de nuevo.

---

## 0. Regla cero: seguridad de credenciales

- La llave privada vive **solo** en `.env` local, nunca en el repo, nunca en un log, nunca
  en un mensaje de chat, nunca en un commit. `.env` está en `.gitignore` desde el commit 1.
- Usa una wallet **nueva y dedicada**, fondeada únicamente con el capital de trading.
  Nunca la wallet principal.
- Nunca hagas `print()` ni `log.info()` de un objeto que pueda contener `POLYMARKET_PRIVATE_KEY`.
  Hay un test que falla si aparece la variable en cualquier output.
- Si un repo de referencia pide pegar la llave privada en algún sitio que no sea un `.env`
  local, es un drainer. No lo uses.

---

## 1. Contexto y estado del ecosistema (agosto 2026)

Datos verificados que **invalidan la mayoría de tutoriales y repos anteriores a mayo 2026**:

- **CLOB V2 en producción desde el 28-abr-2026.** Contratos Exchange nuevos, backend
  reescrito, colateral **pUSD** (ya no USDC.e). Los SDK V1 y las órdenes firmadas V1 no
  funcionan. Todas las órdenes abiertas se borraron en el corte.
- **SDK oficial de Python: `polymarket-client`** (repo `Polymarket/py-sdk`). Requiere
  Python 3.11+. Tipado, async, con `AsyncPublicClient` / `AsyncSecureClient` y
  suscripciones realtime.
- **No uses** `py-clob-client` (V1, retirado) ni `rs-clob-client` (V1, congelado).
  El SDK unificado de Rust **aún no existe** — por eso este proyecto es Python.
- Dominio EIP-712 versión `"2"`.

**Direcciones de contrato: verifícalas tú antes del primer envío.** Las que circulan en
blogs y repos incluyen direcciones V1. Sácalas de `docs.polymarket.com/resources/contracts.md`
y pégalas en `config.py` con la fecha de verificación en un comentario. Una dirección
obsoleta no da un error descriptivo: da un rechazo genérico que depurarás a ciegas.

Cuando dudes de un detalle de la API, **consulta `https://docs.polymarket.com/llms.txt`**
y lee la página `.md` correspondiente. No asumas desde memoria ni desde blogs.

---

## 2. Las tres verdades que definen la estrategia

### 2.1 El precio de liquidación es el TWAP de Chainlink, no Binance

Los mercados cripto liquidan contra TWAPs de **30 s y 60 s** calculados por Chainlink.
Polymarket los retransmite gratis vía RTDS (`wss://ws-live-data.polymarket.com`), sin
credenciales, o vía SDK:

```python
from polymarket import AsyncPublicClient
from polymarket.streams import CryptoPricesChainlinkTwapSpec

async with AsyncPublicClient() as client:
    async with await client.subscribe(
        CryptoPricesChainlinkTwapSpec(window_seconds=30, symbols=["btc/usd"])
    ) as stream:
        async for event in stream:
            ...  # event.payload.value es Decimal; usa event.payload.timestamp
```

Notas operativas: RTDS necesita un frame de texto `PING` cada 5 s si te conectas directo.
No hay snapshot ni replay tras una desconexión. `payload.value` es `Decimal` exacto —
**nunca** lo pases a `float` en la ruta de ejecución (ver §4 para la frontera exacta).

**Binance/Coinbase sigue siendo útil, pero en otro rol**: como *indicador adelantado*.
El TWAP es un promedio rezagado del spot, así que conocer el spot te permite proyectar
hacia dónde va el TWAP. Es una entrada del modelo, no el ancla.

#### 2.1.b El basis: el bug silencioso más caro de este diseño

Binance cotiza BTC**USDT**. Chainlink agrega varias plazas en BTC/**USD**. Entre el
desanclaje de USDT y la prima por plaza hay una diferencia **persistente** de decenas de
dólares.

Si alimentas el modelo con spot de Binance y lo comparas contra un strike del oráculo sin
corregir, ese basis entra íntegro en `ln(S/K)` y produce una señal falsa **constante y
siempre en la misma dirección**. Es el fallo que parece una estrategia rentable durante
dos días y luego devuelve todo con intereses.

Corrección obligatoria, en logs para que sea multiplicativa:

```python
log_basis = λ·log_basis + (1−λ)·ln(chainlink / binance)   # EWMA, half-life ~10 min
spot_en_unidades_de_oráculo = binance · exp(log_basis)
```

Clampea cada observación (p. ej. ±1%) para que un tick corrupto no envenene el estimador,
y **no cotices hasta que el estimador tenga suficientes muestras**. Un basis mal estimado
es peor que ninguno.

Cross-check gratis: RTDS también retransmite Binance (`crypto_prices`, `btcusdt`) por la
misma conexión. Si tu Binance directo y el Binance relayado divergen, el problema es tu
red, no una oportunidad.

### 2.2 Las comisiones obligan a ser maker

```
fee_taker_por_share = 0.07 * p * (1 - p)        # cripto: feeRate = 0.07
```

A `p = 0.50` son **$0.0175 por share** sobre un payoff de $1. Traducción: un trade agresivo
necesita más de 1.75¢ de edge real solo para empatar, antes del spread.

**Los makers pagan cero.** Además existe un programa de rebates para makers (20% en cripto).

Contexto que refuerza la decisión: cripto tiene el feeRate **más alto de todas las
categorías** (0.07 frente a 0.03–0.05 del resto, y 0 en geopolítica). Ese diseño es
deliberado — está puesto para desincentivar precisamente a los bots agresivos en mercados
de minutos. Ir de taker aquí es pelear contra el diseño del producto.

> **Por tanto: este bot es maker-only. Toda orden lleva `post_only=True`.**
> Cruzar el book requiere una excepción explícita y justificada por escrito en el PR.

### 2.3 La latencia pura no es el edge

- Servidores primarios en `eu-west-2` (Londres). Región más cercana **no georrestringida**:
  `eu-west-1` (Irlanda). Reino Unido está en close-only para la API — un VPS en Londres
  no puede abrir posiciones.
  (Polymarket ofrece colocation en `eu-west-2` previo formulario KYC/KYB. Eso implica una
  excepción a la georrestricción que **no está documentada**: no cuentes con ella sin
  confirmación escrita de ellos. Planifica sobre `eu-west-1`.)
- Desde Perú son ~180-220 ms RTT. Con VPS en `eu-west-1`, ~1-10 ms.
- Algunos mercados aplican **retardo de matching deliberado**. Lee
  `market.trading.seconds_delay` antes de asumir cualquier ventaja de velocidad.
  Un status `delayed` en la respuesta **no es un fill**: trátalo como pendiente y sigue
  su desenlace por el stream de usuario.

Competir por microsegundos contra mesas colocalizadas es una batalla perdida. El edge
buscado aquí es **modelo + gestión de inventario**, no velocidad.

**Diagnóstico obligatorio antes de elegir horizonte** (`model/binary.py::delta_per_dollar`):
cuánto se mueve el fair si BTC se mueve $1. En un Up/Down ATM con σ=60%, S=100 000 y
ventana TWAP de 30 s:

| τ restante | Δ fair por $1 | ticks (0.001) por $10 de BTC |
| ---------- | ------------- | ---------------------------- |
| 300 s      | 0.00223       | 22                           |
| 60 s       | 0.00590       | 59                           |
| 10 s       | 0.06135       | **614**                      |

La gamma de un digital ATM explota contra el vencimiento. **Los últimos ~30 segundos son
territorio de latencia pura y ahí se pierde por construcción.** Ese número, no la
intuición, decide en qué épocas cotizamos.

> **La fila de 10 s decía 0.02044 (204 ticks). Es incorrecta**: son exactamente 3× menos
> de lo que da la fórmula de §3.2, equivalente a haber usado w=10 en vez de w=30 (la
> varianza sale 9× menor). Las filas de 300 s y 60 s sí se reproducen exactas. La
> conclusión de esta sección no cambia — se refuerza: a 10 s del cierre la gamma es el
> **triple** de lo que este documento afirmaba. Verificado contra
> `model/binary.py::delta_per_dollar`, cuyos valores ancla `tests/test_model.py`.

---

## 3. Modelo de fair value

Mercado tipo: "¿BTC ≥ K en el momento T?" (binaria cash-or-nothing).

### 3.0 Unidades: todo en SEGUNDOS

**Regla dura: `τ`, `w` y `σ` se expresan siempre por segundo. Nunca en años.**

La v1 de este documento definía `τ` en años (`minutos/525600`) en §3.1 y luego restaba
`w` en segundos en §3.2. Restar segundos de años no da un error: da un número
silenciosamente absurdo. Anualizar es útil solo para *mostrar* σ a un humano; conviértelo
en el borde de presentación, nunca en el cálculo.

```python
SECONDS_PER_YEAR = 365 * 24 * 3600
sigma_s = sigma_anual / sqrt(SECONDS_PER_YEAR)     # para calcular
sigma_anual = sigma_s * sqrt(SECONDS_PER_YEAR)     # solo para logs
```

Test de cordura que debe existir: con σ=60% anual, un movimiento de 1σ en 300 s sobre
BTC a 100 000 son ~$150–300. Si te sale 1e4 veces mayor o menor, tienes este bug.

### 3.1 Fórmula base

Bajo GBM con drift ≈ 0 (correcto en horizontes de minutos/horas):

```
d2   = [ln(S_eff / K) - var/2] / sqrt(var)
fair = N(d2)
```

donde `var` sale de §3.2 y `S_eff` de §3.3. `N` es la CDF normal estándar.

Para mercados "up/down desde el open", `K` es el precio de apertura de la época; el fair
arranca ≈ 0.50 y se mueve rápido conforme `S/K` se separa de 1.

### 3.2 Varianza terminal con TWAP — CORREGIDA

La variable terminal **no es `S_T`**, es el promedio del spot sobre la ventana `w`
(30 o 60 s) que termina en `T`. Promediar reduce la varianza. Tres regímenes:

| Situación                              | Varianza terminal  |
| -------------------------------------- | ------------------ |
| Liquidación puntual (`w = 0`)          | `σ²·τ`             |
| TWAP, ventana **no** abierta (`τ ≥ w`) | `σ²·(τ − 2w/3)`    |
| TWAP, ventana **ya** abierta (`τ < w`) | `σ²·τ³/(3w²)`      |

Derivación de la rama `τ ≥ w`: de ahora al inicio de la ventana acumulas `σ²(τ−w)`;
promediar dentro de la ventana añade `σ²·w/3` (resultado clásico de opción asiática).
Total `σ²(τ − 2w/3)`.

**Continuidad**: en `τ = w` ambas ramas dan `σ²·w/3`. Debe haber un test que lo verifique.

> **La v1 decía `σ²(τ − w/3)`. Es incorrecto.** Verificado por Monte Carlo con 300 000
> trayectorias: la fórmula correcta acierta dentro de ±0.8%; la de la v1 sobreestima la
> varianza entre +3.6% (τ=300 s) y +98.8% (τ=w=60 s). En fair value eso son hasta **3.9¢
> de error** — más que la fee taker de 1.75¢ y casi el doble del colchón de modelo de §3.5.
> Un colchón no protege de un sesgo que lo excede.
>
> La v1 tampoco tenía rama para `τ < w`: su varianza llegaba a cero en `τ = w/3`, es decir
> el fair colapsaba a 0 o 1 **con certeza absoluta a 10 segundos del cierre**.

Chainlink no publica los límites de muestreo, ponderación ni redondeo de sus feeds TWAP,
así que **no intentes reproducir el valor**: consúmelo y modela alrededor de él.

### 3.3 Spot efectivo dentro de la ventana — NUEVO

Con la ventana TWAP abierta, parte de la media que liquidará **ya es un hecho consumado**.
El spot actual solo pesa `τ/w`:

```
si τ ≥ w:   S_eff = S
si τ < w:   S_eff = [(w − τ)·media_observada + τ·S] / w
```

Sin esto, en los últimos 30 segundos comparas el spot crudo contra el strike y ves
movimientos que el valor de liquidación ya no puede seguir. Son señales fantasma, y
llegan justo cuando la gamma es máxima (§2.3). Alimenta `media_observada` con el propio
feed TWAP de RTDS cuando esté disponible; el acumulador local es solo fallback.

### 3.4 Volatilidad

- EWMA de log-retornos del spot sobre **rejilla temporal fija de 1 s**, no tick a tick.
  Los ticks de Binance llegan a ritmo irregular; sobre ticks, tu σ acaba midiendo el
  volumen de mensajes, no el mercado.
- Usa el **mid del top of book**, no el último trade. El último trade te mete bid-ask
  bounce, que el estimador interpretará como volatilidad real e inflará σ de forma
  sistemática.
- Half-life configurable (arranca en 3 minutos).
- Piso y techo duros: la vol explota alrededor de noticias y el modelo se vuelve basura.
  Si `σ` sale de la banda configurada → **retirar cotizaciones**, no cotizar más ancho.
- **Banda de incertidumbre**: tu σ es ruidosa. Calcula el fair promediando sobre un rango
  de σ (±30% inicial). Eso encoge el fair hacia 0.50 en proporción a lo poco que sabes,
  que es exactamente el comportamiento deseado en digitales lejos del dinero — donde
  equivocarse es más caro.

### 3.5 Regla de entrada

```
edge_bruto = fair - precio
edge_neto  = edge_bruto - fee_aplicable - colchon_modelo
```

Como maker el fee es 0, pero **la selección adversa lo sustituye**: te llenan justo cuando
te equivocas. Modela ese coste explícitamente antes de bajar el colchón.

`colchon_modelo` arranca en **3¢** (subido desde 2¢ en la v1) y solo baja con evidencia de
la Fase 0. Razón del cambio: el error residual del modelo TWAP, el error del basis y la
selección adversa se suman, y 2¢ resultaron ser menores que uno solo de esos términos
cuando la fórmula estaba mal. Baja este número con datos, no con optimismo.

### 3.6 Tests de propiedad del modelo — obligatorios

No testees "el precio es 0.5312". Testea las propiedades que deben cumplirse siempre —
es lo que atrapa los errores de signo y de unidades:

- ATM da ≈ 0.50 (ligeramente por debajo, por el término `−var/2`).
- Monótona creciente en `S`.
- Converge al indicador 0/1 cuando `τ → 0`.
- `var(TWAP) < var(puntual)` al mismo horizonte.
- Continuidad de la varianza en `τ = w`.
- `S_eff` con ventana a mitad y `media_observada` conocida da la media ponderada exacta.
- La banda de incertidumbre encoge hacia 0.50 **lejos del dinero**, y cerca del dinero no
  se aleja más de 1¢ (ver el matiz de abajo).
- `delta_per_dollar` crece al acercarse el vencimiento.
- Cordura de unidades (§3.0).
- El estimador de σ recupera una vol conocida inyectada por simulación.

> **Matiz sobre la banda (§3.4).** Este documento pedía «encoge hacia 0.50, nunca lo
> contrario». Con el promedio correcto `E_σ[N(d2(σ))]` eso es **falso cerca del dinero**:
> `d2 ∝ 1/σ` es convexa, así que promediar sobre σ *aleja* ligeramente el fair de 0.50.
> Medido con banda de ±30%: como máximo **0.53¢** (τ=300 s, w=30 s, σ=60%, S−K=−$50).
> Lejos del dinero sí encoge, que es donde §3.4 lo quería. Promediar uniforme en `1/σ`
> en vez de en `σ` empeora el efecto a 1.93¢, así que la rejilla es uniforme en `σ`.
>
> No se "arregla" con un factor de encogimiento ad-hoc: 0.53¢ está muy por debajo del
> colchón de modelo de 3¢ de §3.5, y falsear el promedio para cumplir una propiedad mal
> enunciada es peor que documentar el efecto. El test lo mantiene acotado por debajo de
> 1¢: si crece, es que la banda dejó de ser un promedio sobre σ.

---

## 4. Arquitectura

```
polybtc/
├── CLAUDE.md
├── pyproject.toml               # uv; Python 3.12+
├── .env.example                 # sin valores reales, nunca
├── src/polybtc/
│   ├── config.py                # pydantic-settings; TODO límite de riesgo vive aquí
│   ├── feeds/
│   │   ├── rtds.py              # TWAP Chainlink 30s/60s  ← ANCLA DE LIQUIDACIÓN
│   │   ├── spot.py              # Binance/Coinbase WS     ← indicador adelantado
│   │   └── book.py              # MarketSpec: book, price_change, best_bid_ask,
│   │                            #   tick_size_change (¡el tick size cambia en vivo!)
│   ├── model/
│   │   ├── vol.py               # EWMA sobre rejilla 1s, piso/techo, banda
│   │   ├── basis.py             # ← NUEVO: EWMA log(chainlink/binance)
│   │   ├── twap.py              # varianza por régimen + spot efectivo (§3.2, §3.3)
│   │   ├── binary.py            # N(d2) + delta_per_dollar
│   │   └── fair.py              # fair value + banda + to_execution()
│   ├── strategy/
│   │   ├── quoting.py           # cotización a dos lados, skew por inventario
│   │   └── edge.py              # cálculo de edge neto y umbral
│   ├── execution/
│   │   ├── router.py            # place/cancel/reconcile, idempotencia
│   │   └── heartbeat.py         # dead-man switch
│   ├── risk/
│   │   └── limits.py            # kill switch, exposición, pérdida diaria, staleness
│   ├── paper/
│   │   └── sim.py               # fills simulados contra el book real
│   ├── research/                # notebooks de análisis de Fase 0
│   └── main.py
└── tests/
```

### Reglas de implementación

- **Frontera `Decimal` / `float`, explícita y en un solo sitio.**
  - `Decimal` **obligatorio** en `execution/`, `risk/`, `feeds/` y todo lo que sea precio,
    tamaño, monto o balance. Los tests fallan si aparece `float` ahí.
  - `float` **obligatorio** en `model/`. `Decimal` no hace `erf`, `log` ni `sqrt` de forma
    sensata, y el fair value es aproximado por construcción — fingir precisión exacta ahí
    es teatro que cuesta rendimiento.
  - La conversión vive **solo** en `model/fair.py::to_execution()`, cuantizando al tick
    size vigente. Un `Decimal(str(x))` disperso por el código es cómo se pierde la frontera.
  - El SDK ya te devuelve `Decimal`, así que la frontera existe quieras o no. Hazla
    explícita.
- **Precisión de precio según tick size.** Con tick `0.01`: 2 decimales de precio, 2 de
  tamaño, 4 de monto. El tick puede cambiar en vivo — escucha `tick_size_change` y
  reemplaza el valor guardado; el CLOB rechaza órdenes con tick obsoleto.
- **Verifica `min_order_size` por mercado** (viene en el book, suele ser 5 shares).
  Este número condiciona el sizing entero — ver §5.1.
- **Idempotencia**: cada intento de orden lleva un client-id propio. Un timeout de red no
  es un rechazo — reconcilia contra `get_user_orders` antes de reintentar.
- **Heartbeat obligatorio en producción.** Si el bot deja de latir, el CLOB cancela todas
  las órdenes abiertas. Es tu red de seguridad, no un extra.
- **GTD**: expira 1 minuto antes de lo indicado y debe estar al menos 3 minutos en el
  futuro. Vida útil efectiva mínima ≈ 2 minutos. No sirve para épocas de 5 minutos.
- Batches: `post_orders` acepta 1-15 órdenes; **revisa `ok` en cada entrada**, el batch no
  es atómico.
- `async`/`await` en todo. Realtime solo existe en los clientes async.
- **La capa de estrategia es una función pura** `(book, inventario, params, reloj) →
  TargetQuotes`. Determinista y testeable sin red. Todo el I/O vive en el engine.

---

## 5. Gestión de riesgo y sizing (capital: $100)

### 5.1 El problema de tamaño, dicho de frente

Con `min_order_size = 5 shares`, una orden a `p = 0.50` cuesta **$2.50**. Un quote a dos
lados al tamaño mínimo consume **$5**.

El límite de $5 por posición de la v1 se agota con **una sola ronda de cotización mínima**.
No queda margen para el skew por inventario — que esta misma sección identifica como la
causa de que revienten los market makers amateurs. La estrategia era correcta con un
tamaño que no permitía ejecutarla.

Dos salidas honestas, elige una y anótala en §8:

- **(A) Subir el cap por mercado a $15–20 y quedarse en 1 mercado.** Da ~3–4 rondas de
  quote, suficiente para skew real. Concentra riesgo en un solo mercado, que a este tamaño
  es aceptable.
- **(B) Aceptar que a $100 no cabe market making con inventario** y usar la Fase 2 solo
  para validar la ejecución, no para buscar PnL.

**No elijas (A) antes de la Fase 0.** Si los datos dicen que no hay edge, el tamaño da
igual.

### 5.2 Límites duros (`config.py`, no sugerencias)

| Parámetro                    | Valor inicial       |
| ---------------------------- | ------------------- |
| Nocional máximo por posición | $5 → revisar en §8  |
| Exposición total máxima      | $30                 |
| Pérdida diaria máxima        | $10 → kill          |
| Mercados simultáneos         | 1 al inicio         |
| Staleness de WS              | 3 s → retirar       |
| Basis sin converger          | → no cotizar        |

- **Sizing**: Kelly fraccional a **1/4**, y además capado al 5% del bankroll. Kelly pleno
  con un modelo mal calibrado te lleva a cero. Para una binaria comprada a `p` con
  probabilidad `q`: `f* = (q − p)/(1 − p)`.
- **Kill switch global**: al tocar la pérdida diaria, cancela todo y no vuelve a operar
  hasta reinicio manual. Sin excepciones automáticas.
- **Halt por staleness**: si el TWAP o el book llevan más de 3 s sin actualizar, retira
  cotizaciones. Cotizar con datos viejos es cómo se pierde dinero rápido.
- **Halt por basis**: si el estimador de basis (§2.1.b) no ha convergido o su observación
  instantánea se sale del clamp, retira cotizaciones. Tu ancla está rota.
- **Skew por inventario**: si te cargas de un lado de la binaria, desplaza las cotizaciones
  para descargarte. Sujeto al problema de tamaño de §5.1.

### 5.3 La economía, sin adornos

Un VPS de $6/mes es el **6% del capital cada mes** — necesitas ~100% anual solo para cubrir
infraestructura. A $100, esto no es un negocio.

Como pieza de portafolio que demuestra criterio de ingeniería cuantitativa, vale mucho.
Como fuente de ingresos, el tamaño no da. La Fase 0 existe precisamente para descubrir cuál
de las dos cosas es, sin gastar el capital. El coste de infraestructura debe aparecer en el
reporte de PnL como línea fija, no ignorarse.

---

## 6. Plan por fases — no saltes ninguna

### Fase 0 — Solo observación (2 semanas, $0 en riesgo)

Sin operar. Nada. Conecta TWAP + spot + book y graba todo a Parquet.

Al final, responde con datos:

1. ¿Cuál es la distribución del spread en estos mercados?
2. ¿Con qué frecuencia `|fair − mid|` supera 1.75¢? ¿Y 3¢?
3. ¿Cuánto rezaga el TWAP al spot, y es predecible?
4. ¿Cuánto tiempo sobrevive una orden en el book antes de que el precio la deje atrás?
5. **¿Cuál es el basis medio Binance↔Chainlink y qué tan estable es?** Si oscila más de
   lo que dura tu half-life, tu ancla se mueve y el edge no existe. Grafica el basis en el
   tiempo antes de creer ninguna respuesta a (2).
6. **¿En qué franja de `τ` está el edge?** Cruza la respuesta a (2) con `delta_per_dollar`
   (§2.3). Si el edge solo aparece por debajo de 30 s, es latencia, no modelo, y no es tuyo.

**Si la respuesta a (2) es "casi nunca", el proyecto se detiene aquí y habrás ahorrado
$100 más el VPS.** Ese es un resultado exitoso, no un fracaso.

### Fase 1 — Paper trading (2 semanas)

`paper/sim.py` contra el book real. Supuestos de cola **pesimistas**: asume que te llenan
solo cuando el precio atraviesa completamente tu nivel, y que eres el último de la cola.
Mide PnL con comisiones y coste de infraestructura incluidos.

### Fase 2 — Live mínimo ($20, un mercado)

Maker-only, post-only, tamaño mínimo. El objetivo **no es ganar**: es validar que la
ejecución real coincide con el simulador. Diferencias grandes = el modelo de fills está mal.

### Fase 3 — Escalar

Solo si la Fase 2 es positiva sobre **≥200 trades**. Menos que eso no distingue edge de suerte.

---

## 7. Referencias

**Documentación (fuente primaria, siempre por encima de blogs):**
- `https://docs.polymarket.com/llms.txt` — índice completo
- `/market-data/chainlink-twap.md`, `/market-data/realtime-data.md`, `/trading/fees.md`,
  `/trading/place-orders.md`, `/api-reference/geoblock.md`,
  `/api-reference/trading-rate-limits.md`, `/resources/contracts.md`

**Repos de referencia (leer, no copiar):**

- `warproxxx/poly-maker` — la mejor referencia de **arquitectura**, con una advertencia
  importante: está enfocado en **mercados políticos** y su fair value es un *microprice*
  ponderado por profundidad del propio libro, ajustado por flujo de órdenes.
  Eso es fair value **endógeno**, derivado del book. El nuestro es **exógeno**: viene de un
  oráculo externo. Consecuencia práctica:
  - **Te sirve entero**: estrategia como función pura, ExecutionGateway, journaling para
    replay, reconciliación de órdenes, heartbeat, kill switch, modo paper.
  - **No te sirve nada**: toda su lógica de pricing. En un mercado cripto la información
    está fuera de la plataforma; el microprice solo te dice lo que el book ya sabe.
  - **Verifica su dependencia pinneada tú mismo.** Hay referencias contradictorias sobre
    si usa `py-clob-client` (V1) o `py-clob-client-v2`. Mira el `pyproject.toml` en `main`
    antes de asumir que habla V2.
- `Polymarket/poly-market-maker` — oficial pero anterior a V2. Útil por la lógica de
  bandas y reconciliación de órdenes; **el código de firma está obsoleto**.

**Advertencia:** el espacio "polymarket bot" en GitHub está saturado de repos SEO y
algunos drainers. Antes de usar cualquiera: revisa la fecha del último commit real (no
"Update README"), busca "losing money" o "double-fill" en issues, y verifica que hable
CLOB V2. Si no encuentras lógica de reintentos e idempotencia en 30 segundos de lectura,
fallará en producción.

---

## 8. Decisiones abiertas

Marcar cuando se resuelvan. No implementar asumiendo una respuesta.

- [ ] **Tipo de mercado objetivo**: ¿horarios up/down, épocas de 15 min, o strike fijo
      ("BTC arriba de $X")? Cambia el modelo: strike fijo usa `N(d2)` completo; up/down
      desde el open fija `K` en la apertura de la época.
- [ ] **Ventana TWAP por mercado**: ¿30 s, 60 s, o liquidación puntual? Determina qué rama
      de §3.2 aplica. **Verifícalo en la página de cada mercado, no lo asumas**: elegir mal
      la ventana mete un sesgo de varianza del mismo orden que el bug que corrigió esta v2.
- [ ] **Tratamiento del empate**: ¿el evento es `>` o `>=`? En una binaria cotizada a 0.50
      el empate vale dinero real. Léelo en la descripción del mercado.
- [ ] **Tipo de wallet**: Deposit Wallet (sig type 3, requiere envoltura ERC-7739 de la
      firma) vs Proxy (sig type 1). Determina toda la ruta de firmado. **Resuélvelo antes
      de escribir `execution/`** — no es refactorizable a mitad.
- [ ] **VPS en `eu-west-1`** contratado, o corriendo desde casa (limita a estrategias
      de horizonte más largo).
- [ ] **Sizing: opción (A) o (B) de §5.1**, tras los datos de Fase 0.
- [ ] **Umbral de edge definitivo**, tras los datos de Fase 0.

---

## 9. Cómo trabajar en este repo

- Tests antes de features en `execution/` y `risk/`. Un bug ahí cuesta dinero real.
- **Los tests de propiedad de §3.6 se escriben antes que el resto del modelo.** La v1 de
  este documento llevaba un error de fórmula que ningún test de esa lista habría dejado pasar.
- `ruff` + `mypy --strict`. Sin excepciones en módulos que tocan dinero.
- Todo parámetro configurable en `config.py`, ninguno hardcodeado en la lógica.
- Cada cambio de estrategia va acompañado del análisis de Fase 0/1 que lo justifica.
  "Se me ocurrió" no es justificación.
- Logs estructurados (JSON) de cada decisión: fair, precio, edge, tamaño, motivo, **y los
  insumos del modelo** (σ, basis, `S_eff`, τ, rama de varianza usada). Sin los insumos no
  puedes reconstruir por qué el bot decidió lo que decidió.
- Cuando corrijas una fórmula, **deja la anterior citada en el commit message junto al
  número que la invalidó.** Es cómo se evita reintroducirla seis meses después.

---

## 10. Registro de cambios v1 → v2

| § | Cambio | Motivo |
| --- | --- | --- |
| 1 | Direcciones de contrato: verificar, no copiar | Circulan direcciones V1 en repos y blogs |
| 2.1.b | **Nuevo**: corrección de basis Binance↔Chainlink | Sesgo constante y unidireccional en `ln(S/K)` |
| 2.2 | Añadido el porqué del feeRate 0.07 | Refuerza maker-only: es diseño anti-bot |
| 2.3 | `delta_per_dollar` como diagnóstico obligatorio | Decide el horizonte con números, no intuición |
| 2.3 | Matiz sobre colocation en `eu-west-2` | Contradice el close-only de GB; no documentado |
| 3.0 | **Nuevo**: todo en segundos | La v1 restaba segundos de años entre §3.1 y §3.2 |
| 3.2 | `σ²(τ−w/3)` → `σ²(τ−2w/3)` + rama `τ<w` | Error verificado por Monte Carlo: hasta 3.9¢ |
| 3.3 | **Nuevo**: spot efectivo dentro de la ventana | Sin él, señales fantasma en los últimos 30 s |
| 3.4 | Mid del book (no último trade) + banda de σ | Bid-ask bounce inflaba σ sistemáticamente |
| 3.5 | Colchón 2¢ → 3¢ | 2¢ era menor que el sesgo del propio modelo |
| 3.6 | **Nuevo**: tests de propiedad obligatorios | El bug de §3.2 no habría sobrevivido a uno |
| 4 | Regla `Decimal` → frontera explícita | `Decimal` en el modelo es teatro; en ejecución, obligatorio |
| 5.1 | **Nuevo**: el cap de $5 no permite skew | `min_order_size` × 2 lados agota la posición |
| 5.3 | Economía del VPS con la conclusión completa | ~100% anual solo para cubrir infraestructura |
| 6 | Fase 0: preguntas 5 y 6 (basis, franja de τ) | Sin ellas, la pregunta (2) es interpretable |
| 7 | Matiz sobre `warproxxx/poly-maker` | Fair value endógeno; la arquitectura sí transfiere |
| 8 | Añadidas: ventana TWAP, empate, orden de wallet | Cada una cambia el modelo o bloquea `execution/` |

### v2.1 — al implementar `model/` (septiembre 2026)

Dos números de la v2 que no sobrevivieron al contacto con su propia implementación. Los
detectaron los tests de §3.6, que es exactamente para lo que están.

| § | Cambio | Motivo |
| --- | --- | --- |
| 2.3 | Fila τ=10 s: `0.02044` (204 ticks) → `0.06135` (**614**) | 3× de error, como usar w=10 en vez de w=30. Refuerza la conclusión: la gamma a 10 s es el triple de lo documentado |
| 3.6 | Propiedad de la banda: «nunca lo contrario» → encoge lejos del dinero, acotada en 1¢ cerca | `d2 ∝ 1/σ` es convexa: promediar sobre σ aleja el fair de 0.50 cerca del dinero. Medido: 0.53¢ máx. |
