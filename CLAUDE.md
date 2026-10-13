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

> **Si te conectas directo, NO envíes `filters` en la suscripción.** La documentación
> presenta ese campo como el modo de elegir símbolo; en la práctica es lo que apaga el
> stream. Medido contra el servidor el 2026-09-02, mismo tópico, misma conexión:
>
> | `filters`                        | Acuse | Updates en vivo        |
> | -------------------------------- | ----- | ---------------------- |
> | `"btcusdt,ethusdt"` (lo que dice la doc) | no    | ninguno         |
> | `"{\"symbol\":\"btcusdt\"}"`     | sí    | ~ninguno, intermitente |
> | campo ausente                    | sí    | ~1/s, sostenido        |
>
> Sin `filters` llegan todos los símbolos y se descartan en cliente por `payload.symbol`.
> Es lo que hace el SDK oficial. **El fallo no da error**: la suscripción se acusa y el
> feed se queda permanentemente stale, que es indistinguible de un problema de red.
>
> Dos consecuencias más, verificadas el mismo día: `crypto_prices_chainlink` puede
> responder bajo el tópico `crypto_prices` —el mismo que el relay de Binance—, así que hay
> que enrutar por tópico **y** símbolo; y al suscribirse llega un `type: "subscribe"` con
> `payload.data`, un backfill a 1 s que la doc no describe y que sirve para arrancar con un
> valor bueno. Ante cualquier duda futura sobre el formato de un mensaje, **inspecciona el
> tráfico del SDK oficial**: es el único contraste fiable contra la doc.

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

#### 2.1.c Línea base medida del basis (2026-09-08)

«Decenas de dólares» ya no es una estimación: está medido sobre una hora continua de feeds
(3.548 muestras, `data/fase0-baseline/`). Estos números son la referencia contra la que se
juzgará la pregunta 5 de la Fase 0 y, por tanto, la fila 0 del go/no-go de §6.

| Escala | Observado |
| --- | --- |
| Dentro de una hora | media **−$14,32**, desviación **$0,82**, rango **$2,90** |
| Deriva dentro de esa hora | −12,9 → −15,3 → −13,4 (suave, la sigue una half-life de 10 min) |
| **Entre días** | **−$28,6 (07-sep) → −$14,3 (08-sep)** |

Lo que hay que leer aquí son las dos últimas filas juntas. **Dentro de la hora el basis es
estable y el estimador lo sigue sin problema. Entre días el nivel se movió $14**, así que
no existe un basis «del par BTC» que puedas calibrar una vez y reutilizar: hay que
estimarlo en vivo, siempre, y no cotizar hasta que haya convergido (§5.2).

Traducido a lo único que importa, céntimos de fair (σ=60%, w=60 s, con
`model/binary.py::delta_per_dollar`):

| | τ=300 s | τ=60 s |
| --- | --- | --- |
| Basis SIN corregir ($14,3) | 4,21¢ | 15,17¢ |
| Residuo tras corregir ($2,9) | 0,85¢ | 3,08¢ |

Sin corregir, el basis **por sí solo** fabrica una señal de 4 a 15¢ contra un umbral de
decisión de 3¢: es decir, fabrica exactamente la conclusión «hay edge» que haría gastar el
capital. Corregido, el residuo se queda por debajo del colchón salvo cerca del vencimiento,
donde vuelve a superarlo — otra razón, independiente de la gamma de §2.3, para no cotizar
en los últimos segundos.

### 2.2 Las comisiones obligan a ser maker

```
fee_taker_por_share = 0.07 * p * (1 - p)        # cripto: feeRate = 0.07
```

A `p = 0.50` son **$0.0175 por share** sobre un payoff de $1. Traducción: un trade agresivo
necesita más de 1.75¢ de edge real solo para empatar, antes del spread.

**Los makers pagan cero.** Además existe un programa de rebates para makers (20% en cripto)
— pero **ese rebate no está a tu alcance**: el mercado objetivo pide `rewards_min_size` de
50 shares, o sea $25 a `p=0.50`, por encima del cap de posición de §5.1 incluso en la
opción (A). Cuenta con fee cero, no con el rebate (§8.1).

> **Ese párrafo mezcla dos programas distintos, y la conclusión es falsa.** Verificado el
> 2026-10-13 contra la doc y contra el mercado vivo:
>
> | | Liquidity Rewards | Maker Rebates |
> | --- | --- | --- |
> | Premia | órdenes **en reposo** cerca del mid | volumen **ejecutado** como maker |
> | Mínimo de tamaño | `rewards_min_size` = 50 shares | **ninguno** |
> | Fondeo | pool por mercado (`clob_rewards`) | 20% de las fees de taker del mercado |
> | En `btc-updown-15m` | **`clob_rewards = None`**: sin pool | activo |
>
> `rewards_min_size` es del **primero**, y el rebate del 20% es el **segundo**: se cobra
> por share ejecutada, sin mínimo, y sí estaba a nuestro alcance —
> `rebate_por_share = 0.2 · 0.07 · p · (1−p)`, o sea 0.35¢ a `p=0.50`, el 0.70% del
> nocional por fill. El que tiene el mínimo de 50 shares es el de liquidity rewards, y ese
> **no tiene asignación** en estos mercados: el programa de $1M para cripto TWAP terminó
> en agosto y `rewards_min_size`/`rewards_max_spread` quedaron como parámetros huérfanos.
>
> No cambia «maker-only» —que se sostiene en el fee de taker, no en el rebate—, pero sí
> cambiaba la economía de proveer liquidez, que es lo que midió la Fase 0.b. El rebate
> resultó real y aun así insuficiente: cubre el 15% de la selección adversa medida
> (§2.2.b). Fuente: `docs.polymarket.com/programs/maker-rebates.md`, y
> `FeeSchedule(rate=0.07, taker_only=True, rebate_rate=0.2)` del propio mercado.

#### 2.2.b La selección adversa, medida (Fase 0.b, 2026-10-13)

§3.5 dice que como maker «la selección adversa sustituye al fee» y manda modelar ese coste
explícitamente. Ya está medido, sobre los mismos 1.600.762 segundos de la Fase 0 y con
`research/adverse_selection.py`. Lo que cuesta que te llenen, por share:

| | Céntimos |
| --- | --- |
| Deriva del mid en tu contra a 60 s del fill | **2,265** |
| Neto del fill (medio spread − deriva) | −1,756 |
| Maker rebate | +0,265 |
| **Total por share ejecutada** | **−1,491** |

Control sobre los 1.035.115 segundos **sin** fill: **+0,020¢**. Factor 113 — el coste lo
crea la ejecución, no una tendencia del mercado. No hay tramo de `τ` que se salve (de
2,1¢ a 5,5¢) ni régimen de tick que lo arregle. El veredicto y su lectura están en
`docs/fase0b-informe.md`: **no se provee liquidez en estos mercados**.

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

> **Hay DOS listas de close-only y confundirlas cuesta una mudanza.** Verificado en
> `docs.polymarket.com/api-reference/geoblock.md` el 2026-09-17, y copiado a
> `deploy/geoblock.py` con esa fecha:
>
> | Lista | Qué permite | Quién está |
> | --- | --- | --- |
> | Bloqueo total (OFAC) | nada | Irán, Siria, Cuba, Corea del Norte, regiones de Ucrania |
> | **Close-only en frontend Y API** | **el bot NO abre posiciones** | **Francia, Alemania**, Reino Unido, Italia, Polonia, Singapur, EE. UU., Australia, Brasil, Canadá (varias provincias)… |
> | Close-only solo en frontend | la API queda libre: el bot funciona | Irlanda, **Países Bajos**, Japón, Corea del Sur, Malta |
> | No aparece | sin restricción | Finlandia, España, Lituania, Portugal, Perú… |
>
> Esto es lo que hace que §2.3 señalara `eu-west-1`: **Irlanda está en la lista blanda**, no
> es que estuviera libre. Y tiene una consecuencia incómoda que este documento no recogía:
> **Francia y Alemania están en la lista dura**, que es donde tienen su datacenter europeo
> casi todos los proveedores baratos. Un VPS en Fráncfort o en Lauterbourg graba la Fase 0
> perfectamente y **no puede abrir una posición en el Sprint 7**.
>
> No afecta a la Fase 0 —grabar no es operar y los feeds son públicos—, pero decide dónde
> montar la máquina definitiva: mudarse entre el Sprint 4 y el 7 cambia el perfil de
> latencia con el que se midieron 14 días. `deploy/preflight.py` lo comprueba desde la IP
> real antes de comprometer ningún plazo.
>
> El campo `blocked` que devuelve `polymarket.com/api/geoblock` refleja la política del
> **frontend**: sale `true` en sitios desde los que la API sí funciona. Se enruta por
> `country`, no por ese booleano.
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
│   │   ├── book.py              # MarketSpec: book, price_change, best_bid_ask,
│   │   │                        #   tick_size_change (¡el tick size cambia en vivo!)
│   │   ├── state.py             # FeedState: último valor, EDAD y reconexiones por feed.
│   │   │                        #   La edad es dato de primera clase: es lo que hace
│   │   │                        #   comprobable el halt por staleness de §5.2.
│   │   ├── ws.py                # bucle de conexión común: reconexión, backoff con
│   │   │                        #   jitter y el PING de TEXTO (≠ ping del protocolo WS)
│   │   ├── parsing.py           # json.loads(parse_float=Decimal): es lo que hace que
│   │   │                        #   la regla Decimal de §4 se cumpla desde el primer byte
│   │   └── discovery.py         # época vigente por SLUG CONSTRUIDO y lectura del
│   │                            #   desenlace. Los token ids rotan (§8.1); buscar por
│   │                            #   end_date NO encuentra el mercado (§8.2)
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
│   ├── recorder/                # ESCRIBE. Proceso 1: `python -m polybtc.recorder`
│   │   ├── engine.py            # bucle de 1 Hz: feeds → modelo → fila. `tick()` es puro
│   │   ├── schema.py            # esquema Arrow. Una columna de fair POR HIPÓTESIS (§8)
│   │   ├── sink.py              # Parquet por hora vía staging Arrow IPC (ver abajo)
│   │   ├── status.py            # state.db: `status` (1 fila/s) y `epochs` (1 fila/época)
│   │   ├── epochs.py            # strike en la apertura, media observada y desenlace
│   │   └── halts.py             # banderas de §5.2. En Fase 0 se GRABAN, no se aplican
│   ├── dashboard/               # LEE. Proceso 2: `python -m polybtc.dashboard`
│   │   ├── app.py               # FastAPI + SSE. localhost, sin autenticación
│   │   ├── history.py           # agregados de los paneles desde los Parquet
│   │   └── static/index.html    # una sola página, sin CDN
│   ├── research/                # análisis de Fase 0
│   │   ├── mc_check.py          # Monte Carlo que valida terminal_variance
│   │   └── fase0_report.py      # las 6 preguntas de §6, §8 y el Brier del go/no-go
│   └── main.py
├── scripts/
│   ├── tail_feeds.py            # una línea/s con precios, basis y edad de cada feed.
│   │                            #   Es donde se hace la prueba del corte de red.
│   └── find_market.py           # herramienta de consola sobre feeds/discovery.py
├── deploy/                      # despliegue: un script versionado, NO un agente con SSH
│   ├── preflight.py             # decide si una máquina sirve ANTES de pagar el plazo
│   ├── bootstrap.sh             # idempotente: rehace la máquina desde cero en 5 min
│   ├── geoblock.py              # las tres listas de restricción de Polymarket (§2.3)
│   ├── soak_report.py           # veredicto de 24 h: ¿empieza el contador de 14 días?
│   ├── pull_data.sh             # respaldo en modo PULL, se ejecuta desde tu máquina
│   ├── polybtc-{recorder,dashboard}.service
│   └── README.md                # runbook por fases
├── docs/
│   └── fase0-informe.md         # veredicto de la Fase 0 y su lectura
├── data/                        # NO versionado
│   ├── YYYY/MM/DD/HH.parquet    # una fila por segundo. SIEMPRE completo y legible
│   ├── YYYY/MM/DD/HH.arrow.part # hora en curso. Es el almacén durable real
│   ├── epochs.jsonl             # journal append-only de épocas y desenlaces
│   └── state.db                 # SQLite WAL. El dashboard lo abre en `mode=ro`
└── tests/
```

**Por qué el Parquet no se escribe de forma incremental.** Un Parquet solo es legible con su
footer, y el footer se escribe al cerrar: un `ParquetWriter` abierto una hora y un `kill -9`
a mitad no dejan un archivo incompleto, dejan uno **ilegible** que además rompe la lectura
del directorio entero en el Sprint 5. Por eso cada volcado va a un stream Arrow IPC con
`fsync` —formato de bloques autocontenidos, legible aunque esté truncado— y el Parquet se
escribe de una vez al cerrar la hora, con `rename` atómico. Un apagón cuesta como mucho el
último minuto sin volcar, nunca una hora, y nunca deja basura en `data/`.

**El grabador y el dashboard no comparten nada más que el disco.** Ni memoria, ni imports:
el dashboard no importa un solo módulo de ejecución. Si revienta, el grabador sigue.

**El apagado ordenado tarda entre 6 y 16 segundos**, medido. No está colgado: son los tres
WebSockets cerrando con su handshake (`close_timeout` de `websockets`) antes de que se
vuelque el buffer y se cierre la hora. Consecuencias: el `TimeoutStopSec` de la unidad
`systemd` del Sprint 4 tiene que ser holgado —el de por defecto, 90 s, sobra— y, al
comprobar a mano que un `SIGTERM` dejó el Parquet, hay que **esperar a que el proceso
termine de verdad**. Mirar el directorio a los tres segundos enseña el `.part` todavía
abierto y parece un fallo que no existe.

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
  - **El grabador guarda `float64` en el Parquet, y no es una excepción a la regla.** Va en
    la dirección contraria —`Decimal` del feed a `float` en disco— y de ahí no sale ningún
    precio hacia el CLOB: el Parquet es el sustrato de análisis. Una columna `decimal128`
    llega a pandas como objetos de Python y convierte cualquier media o histograma en
    aritmética de objetos. La conversión vive en un único sitio, `recorder/schema.py::f()`.
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
  > **La edad NO se mide solo con `time.monotonic()`.** En macOS ese reloj no cuenta el
  > tiempo que la máquina pasa suspendida. Medido el 2026-09-08: tras 108 s de suspensión
  > el monótono había avanzado 1 s y todos los feeds figuraban con ~2 s de edad teniendo
  > los datos casi dos minutos — este halt habría dado luz verde sobre datos viejos, que
  > es exactamente lo que existe para impedir. `feeds/state.py` toma el máximo entre el
  > monótono y el reloj de pared: el de pared cubre la suspensión, el monótono cubre un
  > salto de NTP hacia atrás, y el máximo se equivoca siempre hacia declarar stale de más.
  > Corolario para la Fase 0: el grabador no puede vivir en una máquina que se suspenda.
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

> **TERMINADA. 25-sep 04:20 → 11-oct 04:00 UTC. Veredicto: PARAR — no hay edge.**
>
> 1.381.162 filas a 1 Hz, cero huecos, 1.537 épocas con desenlace. El informe completo está
> en `docs/fase0-informe.md` y los números los reproduce
> `src/polybtc/research/fase0_report.py`.
>
> **El modelo predice los desenlaces peor que el precio de mercado en todos los tramos de
> τ** (Brier 0,1598 contra 0,1474), y sigue perdiendo un 6,37% con σ recalibrada a su mejor
> valor posible (×1,5). El `|fair − mid| > 3¢` el 62,6% del tiempo **no era oportunidad,
> era error propio** — y esa es la lección central: esa métrica no distingue «el mercado se
> equivoca» de «yo me equivoco», y la tabla de go/no-go de abajo se construyó sobre ella.
>
> La fila 0 **no** se dispara: el basis corregido deja 1,77¢ de residuo a τ=300 s, por
> debajo del colchón. El ancla estaba bien; lo que no estaba a la altura era el modelo.
>
> No se avanza al Sprint 6. Coste total: ~€6 de VPS, $0 de capital.

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

- [x] **Tipo de mercado objetivo**: familia `btc-updown-15m` (existe también `-5m`).
      **Up/down desde la apertura de la época**: `K` es el precio al inicio del rango, no
      un strike fijo. Verificado el 2026-09-08 en la descripción del propio mercado.
- [x] **Ventana TWAP por mercado**: **w = 60 segundos.** Resuelto contra 1.534 épocas con
      desenlace real: el TWAP de 60 s en `T` acierta el **95,96%**, frente al 82,27% de la
      media del rango de 15 minutos del título. El mercado liquida contra el stream
      `btc-usd-twap-60s` que la propia página cita, no contra lo que dice su título. La
      contradicción del producto se resuelve a favor de la fuente citada, por 14 puntos.
      Detalle en `docs/fase0-informe.md`. Lo que sigue es el enunciado original:
- [ ] ~~**Ventana TWAP por mercado**~~: **sin resolver, y no por descuido.** La descripción
      dice que resuelve por «el TWAP del **rango de tiempo especificado en el título**»
      —15 minutos— pero cita como fuente el stream **`btc-usd-twap-60s`**. Son dos cosas
      distintas y la documentación de Polymarket no cubre cómo resuelven estos mercados.
      Medido con `delta_per_dollar` a τ=60 s: `w=60` da 0.01061 y `w=900` da 0.15913, un
      **factor de 15**. Del mismo orden que el error que corrigió la v2, así que no se
      elige a ojo. **Se decide con datos**: grabar TWAP30/TWAP60 a 1 s junto a la frontera
      y la resolución de cada época, y ver qué hipótesis predice los desenlaces.
      **El experimento ya está montado y corriendo**: el grabador escribe el fair bajo las
      dos hipótesis a la vez (`fair_w60` y `fair_w900`, de
      `Settings.twap_window_hypotheses_s`), más la frontera exacta de cada época, sus
      valores de cierre y su desenlace en la tabla `epochs`. La decisión se toma en el
      Sprint 5 con esos datos delante, no antes. Grabar una sola hipótesis habría obligado
      a acertar antes de tener con qué acertar, y fallar habría invalidado los 14 días.
      Nota: la fuente que cita la propia página del mercado sigue siendo
      `btc-usd-twap-60s`, mientras el título sigue diciendo un rango de 15 minutos —
      verificado de nuevo el 2026-09-08. La contradicción es del producto, no de la lectura.
- [x] **Tratamiento del empate**: el evento es **`>=`**, y el empate **resuelve "Up"**.
      Literal de la descripción: *«greater than or equal to the price at the beginning of
      that range»*. Verificado el 2026-09-08.
Las cuatro que siguen quedan **sin objeto** al cerrarse el proyecto (2026-10-13). No se
«resolvieron»: dependían de llegar a operar, y las tres vías de edge están medidas y
descartadas (§2.2.b y la v2.8). Se dejan escritas porque quien reabra esto necesita saber
qué estaba pendiente, no encontrarse una lista en blanco.

- [—] **Tipo de wallet**: Deposit Wallet (sig type 3, requiere envoltura ERC-7739 de la
      firma) vs Proxy (sig type 1). Determina toda la ruta de firmado. **Resuélvelo antes
      de escribir `execution/`** — no es refactorizable a mitad. Sin objeto: `execution/`
      no se escribió nunca, y es el primer sitio donde habría que empezar.
- [—] **VPS en `eu-west-1`** contratado, o corriendo desde casa (limita a estrategias
      de horizonte más largo). Sin objeto: la Fase 0 corrió en Helsinki —grabar no es
      operar— y la máquina se destruyó el 2026-10-13. Para operar seguiría haciendo falta,
      con el cuidado de las dos listas de close-only de §2.3.
- [—] **Sizing: opción (A) o (B) de §5.1**, tras los datos de Fase 0. Sin objeto: los datos
      dijeron que no se opera, y §5.1 ya avisaba de no elegir antes de tenerlos.
- [—] **Umbral de edge definitivo**, tras los datos de Fase 0. Sin objeto, y con una
      lección detrás: el umbral que importaba no era un número de edge sobre `|fair − mid|`
      sino **la métrica misma** (v2.8).

### 8.1 Hechos del mercado objetivo, verificados el 2026-09-08

No son decisiones: son datos medidos contra la API. Se listan aquí porque tres de ellos
contradicen supuestos que este documento daba por buenos.

| Dato | Valor | Consecuencia |
| --- | --- | --- |
| `minimum_tick_size` | **no es una constante** | Ver el aviso de abajo: sigue al precio, no al mercado |
| `minimum_order_size` | 5 shares | Confirma el problema de tamaño de §5.1 |
| `seconds_delay` | `None` | Sin retardo de matching deliberado (§2.3) |
| `fee_type` | `crypto_fees_v2` | — |
| `rewards_min_size` | **50 shares** | **$25 a p=0.50.** ~~El rebate de maker de §2.2 NO es alcanzable~~ — el dato es correcto, la consecuencia no: es el mínimo del programa de **liquidity rewards**, no del rebate. Y ese programa **no tiene pool aquí** (`clob_rewards = None`, verificado el 2026-10-13), así que el mínimo da igual: no hay nada que repartir |
| `rewards_max_spread` | 4.5 | Del mismo programa sin pool. Parámetro huérfano |
| `fee_schedule` | `rate=0.07, taker_only=True, rebate_rate=0.2` | El rebate de maker sí existe y no tiene mínimo de tamaño (§2.2) |

> **El tick sigue al PRECIO, no al mercado.** Esta tabla registraba `minimum_tick_size =
> 0.01` como un hecho de la familia y sacaba de ahí una conclusión sobre §2.3. Medido el
> 2026-09-11, en la misma familia y el mismo día:
>
> | Época | Precio del Up | `minimum_tick_size` |
> | --- | --- | --- |
> | deep OTM | 0.0005 | **0.001** |
> | tres épocas a la vez, cerca del medio | ~0.51 | **0.01** |
>
> Es lo que §4 ya avisaba —«¡el tick size cambia en vivo!»— y esta sección lo había
> congelado. Tres consecuencias:
>
> - La tabla de ticks de §2.3 (expresada en ticks de 0.001) **se divide por diez solo en el
>   régimen grueso**. Cerca de los extremos vale tal cual.
> - El snapshot `book` del canal `market` **trae `tick_size`**, y es la única forma de
>   conocerlo al conectar: si el mercado no cambia de régimen mientras estás conectado,
>   `tick_size_change` no se dispara nunca y te quedas sin tick. `feeds/book.py` lo siembra
>   del snapshot y lo reemplaza con cada cambio.
> - El grabador escribe `up_tick` y `down_tick` en cada fila. Sin esa columna, medio céntimo
>   de spread en el Parquet es ambiguo: libro pegado al suelo en régimen fino, o libro
>   imposible en régimen grueso. Es la misma cifra con dos lecturas opuestas.
>
> `default_tick_size` en `config.py` es solo un arranque y un respaldo. Nunca un dato.

**Los token ids rotan con cada época y no pueden vivir en `config.py`.** Cualquier proceso
de larga duración tiene que descubrir el mercado de la época siguiente, no tenerlo
configurado. Cómo descubrirlo está en §8.2.

> Esta sección afirmaba además que entre el fin de una época y la creación de la siguiente
> hay **un hueco de ~2 minutos sin ningún mercado abierto**. Es falso, y §8.2 explica de
> dónde salía esa observación.

### 8.2 Cómo se descubre la época, verificado el 2026-09-08 a las 21:48 UTC

Tres correcciones de una vez, y las tres fallaban en silencio: no daban error, daban un
grabador que no encontraba mercado o un strike a un día de distancia.

| Supuesto anterior | Lo medido |
| --- | --- |
| Se busca el mercado listando por `end_date` | **No lo encuentra.** El servidor devuelve 100 elementos por página y hay ocho familias de cripto emitiendo un mercado cada 5 min: los `-5m` llenan la página y el `-15m`, que vence más tarde, se queda fuera. A las 21:48 el mercado de 21:45→22:00 existía y la búsqueda devolvía cero |
| Hay un hueco de ~2 min entre épocas | **No hay hueco.** Las siguientes ocho épocas ya existían y aceptaban órdenes con más de dos horas de antelación. El «hueco» era este mismo fallo de paginación visto desde fuera |
| `state.start_date` es la apertura de la época | Es cuándo se **creó** el mercado: `2026-09-07 22:01:23` para la época del día 8 a las 21:45. Un día de desfase, no seis segundos |

**El slug es `<familia>-<inicio de la época en segundos Unix>`**, y el final es exactamente
`inicio + duración`. Verificado: `btc-updown-15m-1788903900` → 1788903900 = 21:45:00 UTC,
`end_date` = 22:00:00. De ahí sale el modo correcto de descubrir: **construir el slug** y
pedirlo con `list_markets(slug=[...])`. Una petición, determinista, sin paginación de por
medio, y con la frontera de la época conocida en vez de inferida.

Dos consecuencias que el grabador aprovecha:

- **La época siguiente ya está en memoria antes de empezar**, así que se adopta en el
  segundo exacto de su apertura y `K` es el precio de ese segundo. Si la adopción dependiera
  del sondeo de red, el strike llegaría con el retraso del sondeo — y a τ pequeño eso son
  céntimos de fair (§2.3).
- **El desenlace se lee por slug con `get_market`**, también para épocas pasadas. Es la
  variable dependiente del experimento que decide la ventana TWAP de §8.

Cuidado con una trampa al leerlo: una época recién vencida figura `closed=False` con el
libro ya en `0.005/0.995`. El libro sabe el desenlace antes de que la resolución esté
publicada, y leer ese precio como si fuera el desenlace contamina el experimento. Se exige
`closed` o `uma_resolution_status == "resolved"` **y** un par de precios complementario y
pegado a los extremos.

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

### v2.2 — al implementar `feeds/` (septiembre 2026)

La documentación de RTDS describe una suscripción que el servidor acepta y que después no
emite nada. Los tres desvíos se descubrieron contrastando el tráfico del SDK oficial, y
los tres fallan **en silencio**: no hay error, solo un feed que nunca se actualiza.

| § | Cambio | Motivo |
| --- | --- | --- |
| 2.1 | **Nuevo**: no enviar `filters` en la suscripción directa a RTDS | Con `filters` la suscripción se acusa y no llega ni un update; sin él, ~1/s sostenido |
| 2.1 | **Nuevo**: enrutar por tópico **y** `payload.symbol` | `crypto_prices_chainlink` puede responder bajo `crypto_prices`; confundirlos deja el basis de §2.1.b en 1 y devuelve el sesgo entero sin síntoma |
| 2.1 | **Nuevo**: existe un backfill `type: "subscribe"` con `payload.data` | No documentado; permite arrancar con un valor bueno en vez de esperar al primer update |
| 4 | Árbol: añadidos `feeds/state.py`, `feeds/ws.py`, `feeds/parsing.py` y `scripts/` | Sin estar en el árbol, una sesión futura los trataría como código huérfano |

### v2.3 — al terminar `feeds/` (septiembre 2026)

Lo que apareció al conectar contra el servidor real y al dejar el proceso una hora corriendo.

| § | Cambio | Motivo |
| --- | --- | --- |
| 2.2 | El rebate de maker del 20% no es alcanzable | `rewards_min_size` = 50 shares = $25, por encima del cap de §5.1 |
| 5.2 | La edad de un feed se mide con dos relojes, no uno | `time.monotonic()` no cuenta la suspensión en macOS: 108 s dormido se veían como 2 s de edad |
| 8 | Cerradas: tipo de mercado (up/down desde el open) y empate (`>=`, resuelve "Up") | Verificadas en la descripción del mercado |
| 8 | La ventana TWAP queda abierta **con el experimento que la decide escrito** | La descripción dice «TWAP del rango» y cita el stream de 60 s; hay un factor 15 en `delta_per_dollar` entre ambas lecturas |
| 8.1 | **Nuevo**: hechos medidos del mercado objetivo | Tick 0.01 (no 0.001), y los token ids rotan por época con un hueco de ~2 min sin mercado |
| 2.1.c | **Nuevo**: línea base medida del basis | «Decenas de dólares» pasa a ser un número: $0,82 de desviación dentro de la hora, pero $14 de movimiento del nivel entre días |
| 1 | `polymarket-client` entra como dependencia, y pinnea `websockets>=13,<16` | §2.1 obliga a contrastar la doc contra el tráfico del SDK; esa regla es inservible si no está instalado |
| 4 | Árbol: añadidos `scripts/find_market.py` y `data/` | El descubrimiento de mercado es un componente, no un apaño de sesión |

### v2.4 — al construir el grabador y el dashboard (septiembre 2026)

El Sprint del grabador tocó la API de descubrimiento por primera vez de forma continua, y
ahí se cayeron tres supuestos que venían de una medición puntual mal interpretada.

| § | Cambio | Motivo |
| --- | --- | --- |
| 8.2 | **Nuevo**: la época se descubre construyendo el slug, no listando por `end_date` | La primera página del listado son 100 elementos y ocho familias de cripto emiten un mercado cada 5 min: el `-15m` se queda fuera. Medido: el mercado existía y la búsqueda devolvía cero |
| 8.1 | **Retirado**: el «hueco de ~2 min entre épocas» | No existe. Las ocho épocas siguientes ya estaban creadas y aceptando órdenes. El hueco era el fallo de paginación visto desde fuera |
| 8.2 | `state.start_date` no es la apertura de la época | Es la creación del mercado, con **un día** de antelación, no seis segundos. Como `K` es el precio en la apertura, usarlo pondría el strike a un día del que liquida |
| 8 | La ventana TWAP sigue abierta, pero **el experimento ya corre** | El grabador escribe `fair_w60` y `fair_w900` a la vez, con la frontera y el desenlace de cada época. Elegir una habría invalidado los 14 días si se falla |
| 4 | Árbol: `recorder/`, `dashboard/`, `feeds/discovery.py`, `data/` y `state.db` | Sin estar en el árbol, una sesión futura los trataría como código huérfano |
| 4 | **Nuevo**: por qué el Parquet no se escribe incremental | Un Parquet sin footer no es un archivo incompleto, es uno ilegible que rompe la lectura del directorio entero |
| 4 | La regla `Decimal` no se rompe al grabar `float64` en Parquet | Va en la dirección contraria y no alimenta ninguna orden; `decimal128` en pandas es aritmética de objetos |
| 3.3 | La media observada la da el acumulador local, no el feed TWAP de RTDS | El feed es una media móvil sobre `[t−w, t]`; §3.3 pide `[T−w, t]`. Solo coinciden en `t = T`, y para `w=900` no hay feed que valga |

### v2.5 — al añadir el panel de spread (septiembre 2026)

Un panel que parecía cosmético destapó que el tick estaba escrito como constante cuando no
lo es. Se corrige antes de arrancar los 14 días porque después una columna nueva parte el
conjunto de datos en dos.

| § | Cambio | Motivo |
| --- | --- | --- |
| 8.1 | **Corregido**: `minimum_tick_size` no es una constante de la familia | Medido el mismo día: 0.001 en una época deep-OTM y 0.01 en tres épocas cerca del medio. §4 ya avisaba y §8.1 lo había congelado |
| 4 | `feeds/book.py` siembra el tick del snapshot `book`, no solo de `tick_size_change` | El snapshot trae `tick_size`; sin leerlo, un mercado que no cambia de régimen deja el tick sin registrar y el CLOB rechaza la orden sin decir por qué |
| 4 | Grabador: columnas `up_tick` y `down_tick` por fila | Medio céntimo de spread es un libro al suelo con tick 0.001 y uno imposible con 0.01. Sin el tick grabado, la pregunta 1 de §6 no se puede responder |
| 6 | Dashboard: panel de spread, por bucket de τ junto a \|fair − mid\| | La pregunta 1 es el techo de lo que un maker captura, y es lo único que distingue «hay edge a τ bajo» de «el libro se vacía y el mid deja de significar nada» (pregunta 6) |
| 6 | Dashboard: chequeo de complementariedad `up_mid + down_mid − 1` | Canario de calidad del feed de libro. En 14 días eso se quiere ver el día 1, no el día 15 |

### v2.6 — al preparar el despliegue (septiembre 2026)

El despliegue obligó a leer la página de geobloqueo con lupa, y ahí había un matiz que este
documento daba por resuelto de una forma que no era.

| § | Cambio | Motivo |
| --- | --- | --- |
| 2.3 | **Nuevo**: las DOS listas de close-only, literales y fechadas | §2.3 decía «región más cercana no georrestringida: Irlanda» sin decir por qué. La razón es que Irlanda está en la lista **blanda** (solo frontend), mientras que **Francia y Alemania están en la dura**: desde ahí el bot no abre posiciones, y es donde tienen datacenter europeo casi todos los proveedores baratos |
| 4 | Árbol: añadido `deploy/` | Preflight, bootstrap idempotente, unidades de systemd, respaldo en pull e informe de soak |
| 6 | **Nuevo**: el soak de 24 h es una compuerta antes de los 14 días | El grabador nunca había corrido más de 22 min seguidos ni cruzado en vivo un cambio de hora, que es cuando cierra el Parquet |

Y tres detalles que solo aparecieron al ejecutar el preflight de verdad, los tres con
fallo silencioso o engañoso:

| Detalle | Qué pasaba |
| --- | --- |
| `urllib` sin `User-Agent` recibe **403** del borde de Cloudflare | Parecía un geobloqueo. Un preflight que confunde «me han bloqueado» con «no me he presentado» descarta una máquina buena |
| El RTT a los endpoints de Polymarket mide **el nodo de borde de la CDN**, no el mercado | Desde Perú: 59 ms a Polymarket y 246 ms a Binance, con Londres a ~180 ms. Sirve para comparar dos máquinas candidatas, no como medida absoluta |
| El dashboard necesita `ReadWritePaths` pese a abrir `state.db` en `mode=ro` | Una base en WAL necesita su `-shm` **para leerla**. Con el escritor muerto y el sistema de ficheros en solo lectura, la lectura falla y el panel diría «sin grabador» justo cuando el grabador se reinicia |

### v2.7 — al desplegar y correr el soak (septiembre 2026)

El despliegue destapó siete fallos que solo aparecen ejecutando, y el soak de 72 h destapó
dos hallazgos de modelo. Los del despliegue están en `deploy/` con su porqué escrito en el
propio script; los de modelo se anotan aquí porque condicionan el análisis del Sprint 5.

| § | Hallazgo | Consecuencia |
| --- | --- | --- |
| 2.1.c | **Coinbase NO gana a Binance como spot.** Medido sobre 259.167 muestras y 72 h | Coinbase tiene mejor NIVEL (media −$3,26 contra −$18,46) pero PEOR dispersión ($7,79 contra $6,57). §2.1.c ya dice que el nivel lo sigue el estimador y la dispersión no: gana Binance, 6,51¢ contra 7,72¢ de residuo |
| 3.4 | **σ está subestimada.** p50 = 12,8% anual, y un 34,7% del tiempo por debajo del piso de 10% | El modelo sale más extremo que el mercado: 32,9% de sus fair en los extremos contra 23,4% del mercado. De ahí que \|fair − mid\| supere 3¢ el 61% del tiempo, que **no es edge, es calibración** |
| 8 | Evidencia preliminar sobre la ventana TWAP | `w=60` encaja mucho mejor que `w=900`: \|fair − mid\| medio de 7,11¢ contra 16,55¢. No es concluyente — el contraste bueno es contra los desenlaces, y ya hay 288 grabados |

**Por qué ninguno de los tres bloquea la Fase 0.** El grabador captura los INSUMOS crudos:
`chainlink`, los dos spots, el strike, `tau_s` y el libro. Cualquier σ o cualquier fuente de
spot se puede recalcular sobre esa serie a posteriori, así que las columnas de `fair` son
una comodidad y no el producto. Lo irrepetible —los precios y los desenlaces— se está
grabando bien.

Dos avisos para quien lea esto en el Sprint 5:

- **No filtres por «sin halt» sin mirar el desglose.** El 35% de `sigma_fuera_de_banda` es
  el artefacto de calibración de arriba, no ceguera real. Las banderas se graban por motivo
  justo para poder distinguirlo.
- **La escala √t es sospechosa.** σ se estima sobre retornos de 1 s y se extrapola a 900 s.
  Si la vol se agrupa a escala de minutos, esa extrapolación subestima la varianza terminal,
  y encaja con lo observado. Es la primera hipótesis a comprobar.

### v2.8 — cierre de la Fase 0 (octubre 2026)

Catorce días de grabación continua contestan las seis preguntas de §6, cierran §8 y dan el
veredicto. El informe está en `docs/fase0-informe.md`.

| § | Cambio | Motivo |
| --- | --- | --- |
| 6 | **Veredicto: parar.** No se avanza al Sprint 6 | El modelo predice los desenlaces peor que el mercado en TODOS los tramos de τ (Brier 0,1598 contra 0,1474), y sigue perdiendo un 6,37% con σ en su mejor calibración posible |
| 8 | **Cerrada: la ventana TWAP es de 60 s** | 95,96% de acierto contra 1.534 desenlaces, frente al 82,27% de la hipótesis de 15 minutos. Liquida contra el stream que cita la página, no contra lo que dice el título |
| 2.1.c | Confirmado: el basis corregido aguanta | Residuo tras EWMA sin sesgo (media −$0,03) y 1,77¢ a τ=300 s, por debajo del colchón. Exactamente lo que §2.1.c predijo. Y **binance gana a coinbase** sobre esta métrica (6,51 contra 8,21), al revés que sobre la dispersión en crudo |
| 3.4 | σ estaba subestimada **un 50%** | El barrido de calibración la sitúa en ×1,5. No basta: ni siquiera ahí el modelo bate al mercado |
| 3.3 | Validada: el TWAP es predecible casi perfectamente | `twap60` ≈ spot retrasado 32 s con correlación 0,99997. La maquinaria de `S_eff` está bien; el problema no estaba ahí |

**La lección que vale más que el veredicto, y que esta sección no tenía:**

`|fair − mid|` **no distingue «el mercado se equivoca» de «yo me equivoco».** Mide las dos
cosas sumadas. La pregunta 2 de §6 y toda la tabla de go/no-go se construyeron sobre esa
métrica, y por eso resultaron inaplicables: con un 62,6% por encima de 3¢, la fila 3 decía
«seguir» cuando la respuesta correcta era «parar».

Lo que sí distingue es contrastar contra los desenlaces reales — Brier del modelo frente al
del precio de mercado. Esa comparación necesita exactamente lo que la Fase 0 grabó, y es la
que debería encabezar la tabla de decisión de cualquier fase futura.

Corolario metodológico, aprendido por las malas con la elección de spot: **más datos no
arreglan una métrica mal elegida.** Tres veredictos distintos salieron de mirar la
dispersión en crudo con muestras cada vez mayores; en cuanto se midió el residuo tras el
EWMA, la respuesta fue estable a la primera.

### v2.9 — Fase 0.b: ¿paga proveer liquidez? (octubre 2026)

La Fase 0 cerró el edge de información y el de velocidad. Quedaba el estructural —cobrar
por hacer de maker— que no exige ganarle la previsión a nadie. Se mide sobre los datos ya
grabados, sin gastar nada. Informe en `docs/fase0b-informe.md`.

| § | Cambio | Motivo |
| --- | --- | --- |
| 2.2 | **Corregido**: el rebate de maker SÍ era alcanzable | §2.2 y §8.1 mezclaban dos programas. `rewards_min_size` = 50 shares es de **liquidity rewards**; el rebate del 20% va por volumen ejecutado y no tiene mínimo. 0.35¢ por share a `p=0.50` |
| 8.1 | **Corregido**: `rewards_min_size` y `rewards_max_spread` son parámetros huérfanos | `clob_rewards = None` en las cuatro épocas consultadas: el programa de $1M para cripto TWAP terminó en agosto. No hay pool, así que su mínimo es irrelevante |
| 2.2.b | **Nuevo**: la selección adversa, medida | §3.5 mandaba modelar ese coste «explícitamente» y nunca se había hecho. Son **2,265¢ a 60 s** contra un umbral de 0,5¢ escrito antes de medir, y el control sin fill da +0,020¢: el coste lo crea la ejecución |
| — | **Veredicto: no se provee liquidez.** No se ejecuta el paso 3 del plan | Por share ejecutada: −1,756¢ de neto + 0,265¢ de rebate = **−1,491¢**. El rebate cubre el 15% del agujero |

**La lección de esta fase**, que es distinta de la de la v2.8 y vale para cualquier medida
futura: **un test sin control no prueba nada.** 2,265¢ de deriva podrían haber sido el
mercado moviéndose; medir la misma deriva en el millón de segundos sin fill —y obtener
+0,020¢— es lo que convierte una cifra en una conclusión. Es el mismo recurso que hacía
creíble a `mc_check.py` incluir la fórmula retirada en su tabla.

Y un corolario sobre el sesgo propio: este estimador solo ve los fills que ocurren
**porque el precio se movió**, así que está sesgado a favor de parar. En vez de dejarlo
como advertencia, se convirtió en un número: el equilibrio está en que **menos del 34,3%**
de tus fills sean de ese tipo. Una limitación que se escribe como umbral comprobable es
una limitación que la siguiente sesión puede atacar; una que se escribe como párrafo, no.
