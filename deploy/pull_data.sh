#!/usr/bin/env bash
# Trae los datos de la Fase 0 del VPS a tu máquina. **Se ejecuta EN TU MÁQUINA, no en el VPS.**
#
# La dirección importa y no es una manía: en modo *pull*, la máquina alquilada no guarda
# ninguna credencial para llegar a tu casa. Si alguien se hace con el VPS, no se lleva de
# regalo un camino hacia tu equipo. En modo *push* sí.
#
# Y hace falta de verdad: son 14 días de datos irrepetibles —una época que ya resolvió no
# se vuelve a observar nunca— viviendo en una máquina alquilada. Los backups semanales que
# incluya el proveedor dejan hasta 7 días de pérdida posible; esto son 16 MB al día.
#
#   ./deploy/pull_data.sh usuario@vps
#   ./deploy/pull_data.sh usuario@vps --dry-run
#   DEST=~/polybtc-backups ./deploy/pull_data.sh usuario@vps
#
# Para que corra solo cada día, en macOS con launchd o con una entrada de cron:
#   0 9 * * *  /ruta/al/repo/deploy/pull_data.sh usuario@vps >> ~/polybtc-backup.log 2>&1

set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Uso: $0 usuario@host [--dry-run]" >&2
  exit 2
fi

REMOTE="$1"; shift
REMOTE_DIR="${REMOTE_DIR:-/var/lib/polybtc/data/}"
DEST="${DEST:-$HOME/polybtc-backups}"
HOST="${REMOTE##*@}"
TARGET="${DEST}/${HOST}"

mkdir -p "${TARGET}"

# --partial: una copia cortada a mitad se reanuda en vez de empezar de cero.
# Sin --delete a propósito: esto es un archivo histórico, no un espejo. Si algo se borra en
# el VPS —por error o por un disco lleno— la copia lo conserva.
#
# Las opciones se quedan en el mínimo común denominador porque macOS no trae el rsync de
# samba sino `openrsync`, que se anuncia como «rsync version 2.6.9 compatible» y rechaza
# todo lo posterior: `--info=stats1` le hace imprimir la ayuda y salir con error. Este
# script corre en TU máquina (el respaldo va en modo pull), así que el rsync que importa es
# el viejo.
rsync -az --partial --stats "$@" "${REMOTE}:${REMOTE_DIR}" "${TARGET}/"

echo
echo "Destino: ${TARGET}"
du -sh "${TARGET}" 2>/dev/null | sed 's/^/  /'
find "${TARGET}" -name '*.parquet' | wc -l | sed 's/^/  archivos Parquet: /'

# El journal de épocas es el registro inmutable de los desenlaces (la variable dependiente
# del experimento de §8). Si falta, la copia no sirve para lo que existe.
if [[ -f "${TARGET}/epochs.jsonl" ]]; then
  wc -l < "${TARGET}/epochs.jsonl" | sed 's/^/  líneas en epochs.jsonl: /'
else
  echo "  AVISO: no hay epochs.jsonl en la copia" >&2
fi
