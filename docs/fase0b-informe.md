# Informe de la Fase 0.b — veredicto: **parar**

**Pregunta:** descartados el edge de información y el de velocidad (Fase 0), ¿paga el
papel estructural — cobrar por proveer liquidez?

**Respuesta: no.** Y no por poco: el coste de que te llenen es **4,5 veces** el umbral que
se escribió antes de medir.

Los números los genera `src/polybtc/research/adverse_selection.py` sobre los mismos
1.600.762 segundos de la Fase 0 (25-sep 04:20 → 13-oct 16:59 UTC). Este documento es la
lectura. Coste de llegar hasta aquí: **€0**.

---

## El umbral, escrito antes de mirar

De `docs/market-making-plan.md`, sin tocar desde que se escribió:

> Si la selección adversa estimada a 60 s supera **medio tick (0,5¢)**, se para y no se
> mide nada más.

Medido: **2,265¢**. En los dos libros, en todos los tramos de `τ`, sobre 388.810 fills
estimados.

---

## Paso 1 — La aritmética de los rewards

Lo primero que apareció es que `CLAUDE.md` §2.2 mezclaba **dos programas distintos**, y
esa confusión escondía la única fuente de ingresos que sí existe.

| | Liquidity Rewards | Maker Rebates |
| --- | --- | --- |
| Qué premia | Órdenes **en reposo** cerca del mid | Volumen **ejecutado** como maker |
| Mínimo de tamaño | `rewards_min_size` = 50 shares ($25) | **Ninguno** |
| Fondeo | Pool por mercado (`clob_rewards`) | 20% de las fees de taker del propio mercado |
| En `btc-updown-15m` | **`clob_rewards = None`** → sin pool | **Activo** |

Verificado el 2026-10-13 contra `docs.polymarket.com/programs/{liquidity-rewards,maker-rebates}.md`
y contra el mercado vivo, que devuelve
`FeeSchedule(rate=0.07, taker_only=True, rebate_rate=0.2)` y
`MarketRewards(clob_rewards=None, rewards_min_size=50, rewards_max_spread=4.5)`.

Dos consecuencias, una en cada dirección:

- **El programa de liquidity rewards no paga nada aquí.** El de $1M para cripto TWAP
  terminó en agosto y no hay asignación vigente. `rewards_min_size` y `rewards_max_spread`
  siguen configurados, pero sin dinero detrás: son parámetros huérfanos. El retorno de esa
  vía es **0%**, no «inalcanzable por tamaño».
- **El maker rebate sí estaba a nuestro alcance, al revés de lo que decía §2.2.** No exige
  tamaño mínimo: se cobra por share ejecutada.

```
rebate_por_share = 0,2 · 0,07 · p · (1 − p)        # = 0,35¢ a p = 0,50
```

A `p = 0,50` eso es **0,70% del nocional por fill**, que es mucho. La cuenta del paso 1,
con el listón que fijaba el plan (€6/mes de VPS = 1,2% mensual sobre $500):

| | |
| --- | --- |
| $25 desplegados a p≈0,50 | 50 shares |
| Una vuelta completa (comprar y vender) | ~$0,27 de rebate |
| Mínimo de pago diario ($1 pUSD) | ~4 vueltas al día |
| 4 vueltas/día sobre $500 de capital | **~6,4% mensual bruto** |

**El paso 1 pasa el listón con holgura.** Por eso había que medir el paso 2: el bruto no ha
pagado todavía la selección adversa.

---

## Paso 2 — Lo que cuesta que te llenen

### Cómo se estima un fill sin tener los trades

Solo se grabó el *top of book* a 1 Hz. Así que no se mide, se estima: una compra en reposo
a `p = bid(s−1)` se da por ejecutada en `s` si `ask(s) ≤ p` — el mejor ask bajó hasta
nuestro nivel, luego alguien vendió contra él. Simétrico para la venta.

Se reportan dos cifras porque el punto de referencia cambia el significado:

- **deriva** = `mid(s−1) − mid(s+k)`. Selección adversa pura, medida desde el mid **antes**
  del fill. Es la que compara con el umbral.
- **neto** = `mid(s+k) − p`. El resultado a mercado del fill. Con el spread a un tick,
  `neto = medio_spread − deriva`, así que «deriva > 0,5¢» y «neto < 0» son el mismo test.

### El resultado

| Horizonte | n | Deriva media | p50 | Neto medio |
| --- | --- | --- | --- | --- |
| 0 s | 388.810 | 2,092¢ | 1,00¢ | −1,579¢ |
| 10 s | 386.967 | 2,267¢ | 2,00¢ | −1,755¢ |
| 30 s | 381.367 | 2,292¢ | 2,00¢ | −1,781¢ |
| **60 s** | 369.808 | **2,265¢** | 2,00¢ | **−1,756¢** |

**El precio no vuelve.** Lo que a 0 s son 2,09¢ sigue siendo 2,27¢ un minuto después. Eso
es la firma de flujo informado: no es impacto transitorio que se revierta, es información
que se queda en el precio. Si revirtiera, el maker ganaría; no revierte.

Por tramo de `τ` —el horizonte de 10 s es el único que cubre los buckets bajos, porque a
60 s el horizonte ya no cabe dentro de la época—:

| τ | n | Deriva a 10 s | Neto |
| --- | --- | --- | --- |
| 0–30 s | 978 | 5,531¢ | −4,775¢ |
| 30–60 s | 6.164 | 4,247¢ | −3,614¢ |
| 60–120 s | 23.163 | 2,972¢ | −2,442¢ |
| 120–300 s | 83.631 | 2,228¢ | −1,731¢ |
| 300–600 s | 136.475 | 2,122¢ | −1,613¢ |
| 600–900 s | 136.556 | 2,204¢ | −1,689¢ |

**No hay franja que se salve.** El coste empeora cerca del vencimiento —hasta 5,5¢, como
la Fase 0 hacía esperar: ahí el mercado tiene Brier 0,0041— pero ni en el tramo más
benigno, a 5–10 minutos del cierre, baja de 2,1¢ contra un umbral de 0,5¢.

Tampoco se salva el régimen de tick fino, donde el spread es más estrecho en términos
absolutos: con tick 0,001 la deriva es 0,982¢ contra un medio spread de 0,19¢ — **cinco
veces el ingreso**, la misma proporción que en el régimen grueso.

### El control, que es lo que le da dientes al test

Sin un control, 2,27¢ podría ser simplemente que el mercado se mueve. La misma deriva
medida en los **1.035.115 segundos en los que no hubo fill**:

| | Deriva a 60 s |
| --- | --- |
| Condicionado a un fill | **+2,265¢** |
| Control (segundos sin fill) | **+0,020¢** |

Factor **113**. El coste lo crea la ejecución, no una tendencia del mercado. Es el mismo
recurso que `mc_check.py` usaba al incluir la fórmula retirada: un test que no puede salir
negativo no prueba nada.

Y los dos libros, `up` y `down`, dan cifras que coinciden en la tercera cifra decimal
(2,265¢ y 2,267¢) siendo series independientes. Eso es un canario de que el estimador no
está leyendo basura.

---

## La decisión

Juntando los dos pasos, **por share ejecutada**:

| Concepto | Céntimos |
| --- | --- |
| Neto del fill a 60 s (medio spread − deriva) | −1,756 |
| Maker rebate (medido sobre los precios reales) | +0,265 |
| **Total** | **−1,491** |

El rebate del 20% —el ingreso que §2.2 daba por perdido y que resultó estar disponible—
cubre el **15%** del agujero. Cada 100 shares ejecutadas dejan **−$1,49**.

**No se ejecuta el paso 3.** No se añade profundidad al grabador, no se contrata nada, no
se arriesga capital. La línea de market making se cierra aquí.

### Lo que haría falta para reabrirla

El estimador tiene un sesgo conocido y va **en contra** de la estrategia: solo ve los fills
que ocurren **porque el precio se movió**. El flujo desinformado que cruza el spread sin
mover el libro —te llenan, el bid se repone, nadie va a tu contra— es invisible a 1 Hz y
sin datos de operaciones, y es precisamente el flujo del que vive un maker.

Esa objeción es legítima, así que se convierte en un número comprobable. Si una fracción
`f` de tus fills son de los que mueven el libro:

```
neto = f · (−1,491¢) + (1 − f) · (medio_spread + rebate) = f · (−1,491¢) + (1 − f) · (+0,778¢)
```

**El punto de equilibrio está en f = 34,3%.** Es decir: esto solo es viable si **menos de
un tercio** de tus fills son de los que mueven el precio. En un mercado donde el toque
aguanta p50 = 2 s y el spread está pegado al tick el 92,7% del tiempo, esa es una apuesta
sobre la composición del flujo, no sobre nada que estos datos respalden.

Reabrirla cuesta grabar profundidad y operaciones dos semanas (paso 3) **para medir una
sola cosa**: esa `f`. Si alguien retoma esto, que sea con esa pregunta escrita delante, y
con el umbral `f < 34,3%` fijado antes de mirar.

---

## Lo que se corrige en `CLAUDE.md`

| § | Qué decía | Qué es |
| --- | --- | --- |
| 2.2 | El rebate de maker del 20% no es alcanzable porque `rewards_min_size` son 50 shares | Son **dos programas**. El rebate no tiene mínimo de tamaño y sí estaba a nuestro alcance; el que tiene el mínimo de 50 shares es el de liquidity rewards, y ese **no tiene pool** en estos mercados (`clob_rewards = None`) |
| 8.1 | `rewards_min_size` = 50 shares → el rebate no es alcanzable | El dato es correcto; la consecuencia que se sacaba de él, no |

---

## Coste total de la Fase 0.b

**€0.** Dos consultas a la documentación, una al mercado vivo, y 13 segundos de CPU sobre
datos que ya estaban grabados y pagados.

Es el mismo desenlace que la Fase 0 y por el mismo motivo: el umbral estaba escrito antes
de medir, así que el resultado no se pudo renegociar al verlo.
