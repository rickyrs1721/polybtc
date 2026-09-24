#!/usr/bin/env bash
# Deja una máquina Ubuntu limpia lista para grabar la Fase 0. Idempotente: se puede volver
# a ejecutar tantas veces como haga falta y converge al mismo estado.
#
# Esa idempotencia es el objetivo entero de este archivo. Lo que hace falta cuando el VPS se
# muere el día 9 de los 14 no es poder entrar a arreglarlo: es poder rehacerlo en cinco
# minutos y desde cero, sobre otro proveedor si toca. Por eso el despliegue es un script
# versionado y no un agente con SSH ni un servidor MCP — §1 del plan ya decidió que este
# repo no añade superficie de ataque, y la máquina que en el Sprint 7 guardará una llave
# privada es el peor sitio para empezar.
#
# NO arranca los servicios. Arrancar es un paso deliberado y aparte (ver deploy/README.md).
#
#   sudo bash deploy/bootstrap.sh
#
# Variables opcionales:
#   POLYBTC_REPO   repositorio a clonar   (por defecto el de origin)
#   POLYBTC_REF    rama o etiqueta        (por defecto main)

set -euo pipefail

REPO="${POLYBTC_REPO:-https://github.com/rickyrs1721/polybtc.git}"
REF="${POLYBTC_REF:-main}"
APP_USER="polybtc"
APP_DIR="/opt/polybtc"
DATA_DIR="/var/lib/polybtc"
ENV_FILE="/etc/polybtc/polybtc.env"
UV_BIN="/usr/local/bin/uv"

log() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }

if [[ "${EUID}" -ne 0 ]]; then
  echo "Ejecútalo como root: sudo bash deploy/bootstrap.sh" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
log "Paquetes base"
# ---------------------------------------------------------------------------
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq --no-install-recommends \
  ca-certificates curl git ufw chrony unattended-upgrades rsync

# El reloj no es un detalle: la edad de un feed se mide con el máximo entre el reloj
# monótono y el de pared (§5.2). Un reloj a la deriva no da error, da halts falsos — o
# peor, frescura falsa sobre datos viejos.
systemctl enable --now chrony >/dev/null 2>&1 || systemctl enable --now chronyd
systemctl enable --now unattended-upgrades >/dev/null 2>&1 || true

# ---------------------------------------------------------------------------
log "Cortafuegos: solo SSH"
# ---------------------------------------------------------------------------
# El dashboard NO se abre al mundo. Escucha en 127.0.0.1 y se mira por túnel SSH: enseña
# el estado interno del sistema y no tiene autenticación de ninguna clase.
ufw --force default deny incoming >/dev/null
ufw --force default allow outgoing >/dev/null
ufw allow OpenSSH >/dev/null
ufw --force enable >/dev/null
ufw status verbose | sed 's/^/    /'

# ---------------------------------------------------------------------------
log "SSH: solo con llave"
# ---------------------------------------------------------------------------
install -d -m 0755 /etc/ssh/sshd_config.d
cat > /etc/ssh/sshd_config.d/10-polybtc.conf <<'SSHD'
# Escrito por deploy/bootstrap.sh
PermitRootLogin no
PasswordAuthentication no
KbdInteractiveAuthentication no
SSHD
if sshd -t; then
  systemctl reload ssh 2>/dev/null || systemctl reload sshd
else
  echo "    configuración de sshd inválida: no se recarga" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
log "Usuario de servicio: ${APP_USER}"
# ---------------------------------------------------------------------------
if ! id -u "${APP_USER}" >/dev/null 2>&1; then
  useradd --system --create-home --shell /usr/sbin/nologin "${APP_USER}"
fi
install -d -o "${APP_USER}" -g "${APP_USER}" -m 0750 "${DATA_DIR}" "${DATA_DIR}/data"

# ---------------------------------------------------------------------------
log "uv"
# ---------------------------------------------------------------------------
# Se instala a /usr/local/bin y no al home del usuario para que las unidades puedan usar
# ProtectHome=yes sin quedarse sin intérprete.
if [[ ! -x "${UV_BIN}" ]]; then
  curl -LsSf https://astral.sh/uv/install.sh | \
    env UV_INSTALL_DIR=/usr/local/bin INSTALLER_NO_MODIFY_PATH=1 sh
fi
"${UV_BIN}" --version | sed 's/^/    /'

# ---------------------------------------------------------------------------
log "Repositorio en ${APP_DIR}"
# ---------------------------------------------------------------------------
if [[ -d "${APP_DIR}/.git" ]]; then
  git -C "${APP_DIR}" remote set-url origin "${REPO}"
  git -C "${APP_DIR}" fetch --quiet origin "${REF}"
  git -C "${APP_DIR}" checkout --quiet "${REF}"
  git -C "${APP_DIR}" reset --quiet --hard "origin/${REF}"
else
  git clone --quiet --branch "${REF}" "${REPO}" "${APP_DIR}"
fi
chown -R "${APP_USER}:${APP_USER}" "${APP_DIR}"
git -C "${APP_DIR}" log --oneline -1 | sed 's/^/    /'

# ---------------------------------------------------------------------------
log "Entorno de Python"
# ---------------------------------------------------------------------------
# `--frozen` a propósito: instala exactamente lo que dice uv.lock. Una resolución distinta
# entre tu máquina y el VPS es cómo se descubre en el día 9 que el servidor lleva otra
# versión del SDK.
cd "${APP_DIR}"

# El Python lo pone uv, NO el del sistema, y se instala en un paso aparte para que un fallo
# aquí se vea aquí. `.python-version` fija 3.12: una imagen reciente de Ubuntu trae un
# Python más nuevo del que puede haber ruedas de `pyarrow`, y sin ruedas uv intentaría
# compilarlo desde fuente en una máquina de 2 vCPU. Eso no falla rápido: falla en veinte
# minutos y con un error de compilador.
sudo -u "${APP_USER}" env HOME="/home/${APP_USER}" "${UV_BIN}" python install
sudo -u "${APP_USER}" env HOME="/home/${APP_USER}" "${UV_BIN}" sync --frozen --no-dev
sudo -u "${APP_USER}" "${APP_DIR}/.venv/bin/python" -c \
  "import polybtc, sys; print(f'    polybtc listo sobre Python {sys.version.split()[0]}')"

# ---------------------------------------------------------------------------
log "Configuración"
# ---------------------------------------------------------------------------
# Sin secretos: la Fase 0 son feeds públicos. La llave privada del Sprint 7 NO vive aquí
# (§0), y cuando llegue el momento habrá que decidir explícitamente si vive en esta máquina.
install -d -m 0755 /etc/polybtc
if [[ ! -f "${ENV_FILE}" ]]; then
  install -m 0640 -o root -g "${APP_USER}" \
    "${APP_DIR}/deploy/polybtc.env.example" "${ENV_FILE}"
fi
sed 's/^/    /' "${ENV_FILE}"

# ---------------------------------------------------------------------------
log "Unidades de systemd"
# ---------------------------------------------------------------------------
install -m 0644 "${APP_DIR}/deploy/polybtc-recorder.service" /etc/systemd/system/
install -m 0644 "${APP_DIR}/deploy/polybtc-dashboard.service" /etc/systemd/system/
systemctl daemon-reload
systemctl enable polybtc-recorder.service polybtc-dashboard.service >/dev/null

cat <<'DONE'

==> Listo. Los servicios están habilitados pero NO arrancados.

    Antes de arrancar, corre el preflight — decide si esta máquina sirve y todavía
    estás a tiempo de cambiarla:

        cd /opt/polybtc && sudo -u polybtc .venv/bin/python -m deploy.preflight

    Si el veredicto es OK:

        sudo systemctl start polybtc-recorder polybtc-dashboard
        systemctl status polybtc-recorder --no-pager

    El contador de los 14 días NO empieza aquí: empieza cuando el soak de 24 h pase
    limpio (deploy/soak_report.py).
DONE
