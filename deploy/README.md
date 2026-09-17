# Despliegue de la Fase 0

Runbook para dejar el grabador y el dashboard corriendo 24/7 durante los 14 días de §6.

**Por qué esto es un script y no un agente.** Lo que hace falta cuando el VPS se muere el
día 9 de los 14 no es poder entrar a arreglarlo: es poder **rehacerlo desde cero en cinco
minutos**, sobre otro proveedor si toca. Eso lo da un script idempotente versionado; no lo
da un servidor MCP, un flujo de N8N ni un agente con acceso SSH. §1 del plan ya decidió que
este repositorio no añade superficie de ataque, y la máquina que en el Sprint 7 guardará
una llave privada es el peor sitio posible para empezar a hacerlo. El efecto secundario
vale tanto como el principal: **cambiar de proveedor cuesta una tarde**, así que la
elección del VPS deja de ser difícil de revertir.

---

## Antes de nada: dónde puede vivir la máquina

Polymarket tiene **dos** listas de «close-only» y confundirlas cuesta una mudanza
(`deploy/geoblock.py` las tiene, consultadas el 2026-09-17):

| Lista | Qué significa | Quién está |
| --- | --- | --- |
| Bloqueo total | Ni leer ni operar | Irán, Siria, Cuba, Corea del Norte, regiones de Ucrania |
| **Close-only en frontend Y API** | **El bot NO puede abrir posiciones** | **Francia, Alemania**, Reino Unido, Italia, Polonia, Singapur, EE. UU., Australia, Brasil… |
| Close-only solo en frontend | La API queda libre: el bot funciona | Irlanda, **Países Bajos**, Japón, Corea del Sur |
| (sin aparecer) | Sin restricción | Finlandia, España, Lituania, Portugal, Perú… |

**Nada de esto afecta a la Fase 0**: grabar no es operar y los feeds son públicos. Decide
dónde montar la máquina **definitiva**, porque mudarse entre el Sprint 4 y el 7 cambia el
perfil de latencia con el que se midieron 14 días.

---

## Fase 0 — Preflight, con el plazo corto todavía sin renovar

```bash
# En el VPS recién creado, tras bootstrap.sh
cd /opt/polybtc
sudo -u polybtc .venv/bin/python -m deploy.preflight            # una hora
sudo -u polybtc .venv/bin/python -m deploy.preflight --minutes 5 # tanteo rápido
```

Comprueba geobloqueo, reloj, alcance y —lo que de verdad decide— **cuánto edge fantasma
fabrica la ubicación**: si el libro llega más viejo que el spot, se ve movimiento que ya no
existe, y eso entra entero en `|fair − mid|` sin que haya ninguna oportunidad. El script lo
traduce a céntimos de fair, que es la única unidad comparable contra el colchón de 3¢.

Si el veredicto es `FALLA`, **prueba otra región antes de renovar el plazo**.

---

## Fase 1 — Bootstrap

```bash
ssh usuario@vps
sudo apt-get update && sudo apt-get install -y git
git clone https://github.com/rickyrs1721/polybtc.git /tmp/polybtc
sudo bash /tmp/polybtc/deploy/bootstrap.sh
```

Idempotente: se puede repetir. Deja usuario de servicio, cortafuegos con solo SSH, SSH sin
contraseña, `chrony`, `uv`, el repo en `/opt/polybtc`, el entorno con `uv sync --frozen`,
la configuración en `/etc/polybtc/polybtc.env` y las dos unidades habilitadas —pero **no
arrancadas**.

Para actualizar más adelante, el mismo script:

```bash
sudo bash /opt/polybtc/deploy/bootstrap.sh
sudo systemctl restart polybtc-recorder polybtc-dashboard
```

---

## Fase 2 — Arrancar

```bash
sudo systemctl start polybtc-recorder polybtc-dashboard
systemctl status polybtc-recorder --no-pager
journalctl -u polybtc-recorder -f
```

Para mirar el dashboard, **túnel SSH** — nunca se abre al mundo, porque enseña el estado
interno del sistema y no tiene autenticación:

```bash
ssh -N -L 8000:127.0.0.1:8000 usuario@vps   # y abre http://127.0.0.1:8000
```

Diagnóstico rápido sin navegador:

```bash
sudo -u polybtc sqlite3 /var/lib/polybtc/data/state.db \
  'select updated_at, ticks, epoch_slug, round(tau_s) tau, strike_source, halt_reasons from status;'
```

---

## Fase 3 — Copia de seguridad, **desde tu máquina**

```bash
./deploy/pull_data.sh usuario@vps
```

En modo *pull* a propósito: así el VPS no guarda ninguna credencial para llegar a tu casa.
Son 16 MB al día contra 14 días de datos irrepetibles — una época que ya resolvió no se
vuelve a observar nunca. Automatízalo a diario en tu Mac con `cron` o `launchd`.

---

## Fase 4 — Soak de 24 h. **El contador de 14 días NO empieza aquí**

```bash
uv run python -m deploy.soak_report --data-dir /var/lib/polybtc/data
```

Existe porque hay diferencia entre «el código funciona» y «esto aguanta un día sin que
nadie lo mire». Cuando se escribió, el grabador nunca había corrido más de 22 minutos
seguidos ni había cruzado en vivo un cambio de hora — que es justo cuando cierra el Parquet
y hace el `rename` atómico.

Comprueba duración, continuidad del segundero, cambios de hora, crecimiento en disco,
reconexiones, halts, fair calculado bajo las dos hipótesis de §8, y las épocas: cuántas
fijaron el strike en la apertura y cuántas tienen desenlace leído. Los umbrales están
escritos **antes** de mirar los resultados, igual que la tabla de go/no-go de §6.

Solo con veredicto `OK` empieza el contador.

---

## Fase 5 — Los 14 días

Revisa el dashboard una vez al día. Si aparecen huecos, arregla la reconexión y **reinicia
el contador**: datos con agujeros dan respuestas falsas (§6).

No toques el esquema del grabador durante la corrida. Una columna nueva parte el conjunto
de datos en dos, y el análisis del Sprint 5 los lee juntos.

---

## Lo que queda pendiente para el Sprint 7

Cuando llegue la wallet, esta máquina pasa a guardar una llave privada y su modelo de
amenaza cambia entero. Antes de eso hay que decidir explícitamente —no heredar por
descuido— si la llave vive aquí o la firma se queda en otro sitio, y revisar `fail2ban`,
el acceso SSH y quién más tiene credenciales del proveedor. Hoy no hay ningún secreto en
esta máquina, y esa es exactamente la razón por la que se puede montar, romper y rehacer
sin miedo durante las próximas cinco semanas.
