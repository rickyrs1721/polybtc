# Fase 0.b — ¿paga la provisión de liquidez?

**Rama:** `research/market-making` · **Estado: TERMINADO — veredicto PARAR** · **Coste: €0**

> **Pasos 1 y 2 hechos el 2026-10-13. La selección adversa a 60 s es de 2,265¢ contra el
> umbral de 0,5¢ escrito abajo: 4,5 veces por encima, en todos los tramos de τ y en los dos
> libros.** El rebate de maker resultó ser real —§2.2 lo daba por inalcanzable confundiendo
> dos programas— pero cubre solo el 15% del coste. **No se ejecuta el paso 3.** El informe
> es `docs/fase0b-informe.md` y los números los reproduce
> `uv run python -m polybtc.research.adverse_selection --data-dir ~/polybtc-backups/<host>`.
>
> Lo que reabriría la línea está escrito como umbral, no como párrafo: este estimador solo
> ve los fills causados por un movimiento de precio, y el equilibrio está en que **menos
> del 34,3%** de los fills reales sean de ese tipo. Medir esa fracción es lo único que
> justificaría el paso 3.

---

## Por qué existe este documento

La Fase 0 respondió una pregunta y cerró una puerta: **no podemos predecir los desenlaces
mejor que el mercado** (Brier 0,1598 contra 0,1474, y 6,37% peor incluso con σ en su mejor
calibración posible). Esa puerta queda cerrada con datos y no se vuelve a abrir afinando el
modelo.

Pero respondió *solo* esa pregunta. Un edge puede venir de tres sitios y solo descartamos
dos:

| Edge | ¿Lo tenemos? | Evidencia |
| --- | --- | --- |
| **Información** | No | A τ≤30 s el mercado tiene Brier 0,0041. No hay hueco donde meter nada |
| **Velocidad** | No | La discrepancia crece con τ, no decrece. Y la colocation exige KYC/KYB empresarial |
| **Papel estructural** | **Sin medir** | Cobrar por proveer liquidez no exige ganarle la previsión a nadie |

El tercero es el único cuya premisa sobrevive al resultado de la Fase 0, y el único que
cambia con $500 en vez de $100: `rewards_min_size` son 50 shares ≈ $25, que eran el 25% del
capital anterior y son el 5% del nuevo. Por primera vez cabe sostener inventario.

**Este documento NO afirma que funcione.** Afirma que no lo sabemos y describe cómo
averiguarlo gastando cero.

---

## El umbral, escrito ANTES de medir

Esto es lo que hace honesto el ejercicio. §6 ya lo dice para el go/no-go y la Fase 0
demostró por qué importa: con `|fair − mid|` superando 3¢ el 62,6% del tiempo, la tabla de
decisión decía «seguir» cuando la respuesta era «parar».

> **Umbral de parada del paso 2:** si la selección adversa estimada a 60 s supera **medio
> tick (0,5¢)**, se para y no se mide nada más.

El razonamiento, para que no se pueda renegociar después: como maker en el toque, lo máximo
que capturas es del orden de medio spread. Con el spread al tick el 92,7% del tiempo, eso
son **0,5¢ por viaje de ida y vuelta**. Si que te llenen te cuesta más que eso, pierdes en
cada operación **antes** de cualquier reward, y ningún programa de incentivos arregla una
estrategia que pierde en su núcleo.

---

## Los pasos

### Paso 1 — La aritmética de los rewards *(coste: 0, sin código)*

Leer la documentación vigente del programa de rewards. **No suponerla** — la skill
`polymarket-api` existe por esto, y la Fase 0 se encontró tres supuestos falsos sobre la
API, todos con fallo silencioso.

Hay que salir con tres números:

1. La fórmula de reparto (habitualmente cuota sobre la liquidez cualificada).
2. Qué cuenta como liquidez cualificada: `rewards_max_spread` = 4,5 y `rewards_min_size` =
   50 shares son los valores medidos el 2026-09-08, hay que reverificarlos.
3. El retorno teórico sobre $25–50 desplegados, en porcentaje mensual.

Contraste: sobre $500 de capital, un VPS de €6/mes es el **1,2% mensual**. Si el retorno
teórico bruto no supera eso con holgura, no hay nada que medir — y recuerda que el bruto
todavía no ha pagado la selección adversa.

### Paso 2 — Selección adversa, con los datos que ya hay *(coste: 0)*

**1,6 millones de filas en `~/polybtc-backups/62.238.113.213/` ya contienen la respuesta.**

Método, con su limitación por delante: solo grabamos el *top of book*, no operaciones ni
profundidad. Así que no se mide, se **estima**: una orden en reposo en el bid a precio `p`
se considera ejecutada cuando el mejor ask baja a `≤ p` (alguien vendió contra ella).
Entonces se mira el mid a 10, 30 y 60 segundos y se compara con `p`.

```
coste_seleccion_adversa = E[ mid(t+k) − p | ejecutada en t ]
```

Si sale sistemáticamente negativo para el lado comprador, te llenan justo cuando el precio
se va en tu contra. Que es la definición del problema.

Hay que reportarlo **por bucket de τ**: la Fase 0 ya enseñó que el mercado es casi perfecto
cerca del vencimiento, y es de esperar que ahí la selección adversa sea máxima. Puede que
la estrategia solo sea viable lejos del cierre — o en ninguna parte.

### Paso 3 — Solo si 1 y 2 sobreviven *(coste: €6, o €0 en hardware propio)*

Añadir **profundidad del libro** al grabador. Hoy guardamos mejor bid y mejor ask; sin
tamaños no se puede estimar la cuota de rewards, porque no sabes contra cuánta liquidez
compites. Son columnas nuevas, así que hay que hacerlo **antes** de empezar a medir: una
columna añadida a mitad parte el conjunto de datos en dos.

Dos semanas. El mismo rigor: cero huecos o se reinicia el contador.

### Paso 4 — Decidir

Con el umbral del paso 2 ya escrito, y uno nuevo para el 3 escrito **antes** de mirar sus
resultados.

---

## Lo que la Fase 0 ya sabe y no juega a favor

No son motivos para no medir. Son el contexto con el que hay que leer lo que salga.

| Dato medido | Qué implica para un maker |
| --- | --- |
| Spread al tick el **92,7%** del tiempo | No hay sitio donde ponerse *dentro* del spread. Solo unirse al toque y hacer cola |
| El toque aguanta **p50 = 2 s** | La cola se reordena antes de que tu orden madure |
| El mercado valora mejor que tú en todos los tramos | Estás dando liquidez a un flujo mejor informado |
| `min_order_size` = 5 shares ($2,50) | Con $500 ya hay granularidad real para gestionar inventario |

Es el escenario difícil del market making, no el fácil. Eso no lo descarta: lo encarece.

---

## Estado para retomar

- **Rama:** `research/market-making`, desde `1adcbdb`.
- **Datos:** `~/polybtc-backups/62.238.113.213/` — 1.600.762 filas, 1.779 épocas resueltas,
  445 Parquet. Verificado: `uv run python -m polybtc.research.fase0_report --data-dir <ruta>`
  reproduce el informe completo sin tocar el VPS.
- **VPS: destruido el 2026-10-13**, junto con su Primary IP. No queda nada corriendo ni
  facturando. Los pasos 1 y 2 se hicieron sobre la copia local, que es la que queda como
  único archivo de la Fase 0. Pendiente solo de confirmar que la **deploy key de GitHub**
  se borró también (`Settings → Deploy keys`): sin máquina que la use, sobra.
- ~~**Siguiente acción concreta:** el paso 2.~~ Hecho. Cerró la línea entera por sí solo,
  que es exactamente lo que esta línea anticipaba que podía pasar.
