"""pydantic-settings: credenciales, contratos V2 y TODO límite de riesgo (§5.2).

Regla de §9: ningún parámetro se hardcodea en la lógica. Todo lo que un humano pueda
querer cambiar sin editar código vive aquí.

La llave privada **no** se lee de `./.env`: vive en `~/.polybtc/.env`, fuera del árbol del
repo, para que ni un `git add -A` accidental ni una regla de lectura mal escrita la
alcancen (§0). Este módulo aún no declara ningún campo secreto — los feeds de la Fase 0
son públicos y sin credenciales — pero la ruta ya apunta donde debe.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

SECRETS_ENV_PATH = Path.home() / ".polybtc" / ".env"
"""Fuera del repo a propósito (§0). No se crea aquí; no hace falta hasta el Sprint 7."""

REPO_ROOT = Path(__file__).resolve().parents[2]
"""Raíz del repo. `data/` cuelga de aquí y NO se versiona (§4)."""


class Settings(BaseSettings):
    """Configuración del proceso. Se instancia una vez y se pasa hacia abajo."""

    model_config = SettingsConfigDict(
        env_file=(SECRETS_ENV_PATH,),
        env_prefix="POLYBTC_",
        extra="ignore",
    )

    # -- Feeds: URLs verificadas en la documentación --------------------------------
    # docs.polymarket.com/market-data/realtime-data.md   (consultado 2026-09-02)
    # docs.polymarket.com/market-data/chainlink-twap.md  (consultado 2026-09-02)
    rtds_url: str = "wss://ws-live-data.polymarket.com"

    # docs.polymarket.com/asyncapi.json, canal `market` (consultado 2026-09-02)
    clob_market_ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    # developers.binance.com/docs/binance-spot-api-docs/web-socket-streams
    # (consultado 2026-09-02). Raw stream: {base}/{symbol}@bookTicker
    binance_ws_base_url: str = "wss://stream.binance.com:9443/ws"

    # docs.cdp.coinbase.com/exchange/websocket-feed/channels (consultado 2026-09-24)
    coinbase_ws_url: str = "wss://ws-feed.exchange.coinbase.com"
    coinbase_product_id: str = "BTC-USD"
    """**BTC-USD, no BTC-USDT.** Misma denominación que Chainlink, así que la mitad del
    basis de §2.1.b —el desanclaje de USDT— desaparece de raíz."""

    # -- Símbolos -------------------------------------------------------------------
    chainlink_symbol: str = "btc/usd"
    """Minúsculas y con barra, tal como los acepta el filtro de RTDS."""

    binance_symbol: str = "btcusdt"
    """Minúsculas para el nombre del stream y para el filtro del relay de RTDS."""

    twap_windows_s: tuple[int, ...] = (30, 60)
    """Únicos valores admitidos por RTDS. Cuál se usa para cotizar es decisión de §8."""

    spot_source: Literal["binance", "coinbase"] = "binance"
    """Qué feed alimenta σ, el basis y el `fair`. **Los dos se graban siempre.**

    Esto es un conmutador, no una preferencia: ambos spots van al Parquet en cada fila, y
    cuál sirve mejor lo deciden los datos de la Fase 0, igual que las dos hipótesis de
    ventana TWAP de §8. Lo que cambia esta opción es solo de cuál se fía el modelo.

    Sigue en `binance` porque es la fuente con la que se midió la línea base del basis de
    §2.1.c. Moverlo a `coinbase` obliga a volver a medir esa línea base: es otra plaza y
    otra denominación, así que el basis será otro número.
    """

    # -- Book -----------------------------------------------------------------------
    market_slug_prefix: str = "btc-updown-15m"
    """Familia de mercados objetivo (§8). Existe también `btc-updown-5m`.

    Es un PREFIJO, no un slug: cada época crea su propio mercado con el epoch al final.
    """

    epoch_seconds: float = 900.0
    """Duración de una época de la familia `btc-updown-15m`.

    Es lo que define la frontera: el inicio se deriva como `end - epoch_seconds`, no del
    `start_date` que publica Gamma, que llega hasta 6 s tarde (§8.1) y metería ese desfase
    dentro del strike.
    """

    token_ids: tuple[str, ...] = ()
    """Outcome token ids del mercado seguido, en orden `(Up, Down)`.

    Vacío a propósito, y **no es algo que deba rellenarse a mano de forma permanente**:
    los token ids rotan con cada época (§8.1). Sirve para diagnóstico puntual vía
    `POLYBTC_TOKEN_IDS`; un proceso de larga duración tiene que descubrir el mercado de la
    época vigente, no leerlo de aquí.
    """

    # -- Salud de los feeds (§5.2) ---------------------------------------------------
    staleness_s: float = 3.0
    """Más de esto sin actualizar un feed → retirar cotizaciones (§5.2)."""

    rtds_text_ping_interval_s: float = 5.0
    """RTDS exige un frame de TEXTO `PING` cada 5 s. No es el ping del protocolo WS."""

    ws_backoff_base_s: float = 0.5
    ws_backoff_max_s: float = 30.0
    ws_open_timeout_s: float = 10.0
    ws_recv_timeout_s: float = 10.0
    """Sin mensajes durante este tiempo se considera conexión muerta y se reconecta.

    RTDS no tiene replay ni snapshot tras una desconexión (§2.1): colgarse de un socket
    silencioso es peor que reconectar, porque la edad del feed se dispara sin que nadie
    lo note.

    Bajado de 30 s tras la corrida de una hora del 2026-09-08. **RTDS se queda mudo sin
    cerrar la conexión**: hubo un episodio en el que dejó de emitir y solo este timeout lo
    detectó, con 33 s de ceguera. Sin `filters` llegan todos los símbolos, del orden de
    decenas de mensajes por segundo, así que 10 s de silencio ya es un orden de magnitud
    por encima de lo normal y no genera reconexiones espurias.
    """

    # -- Modelo: basis (§2.1.b) ------------------------------------------------------
    basis_half_life_s: float = 600.0
    basis_clamp: float = 0.01
    basis_min_samples: int = 30

    # -- Modelo: volatilidad (§3.4) --------------------------------------------------
    vol_half_life_s: float = 180.0
    vol_grid_s: float = 1.0
    vol_floor_annual: float = 0.10
    vol_ceiling_annual: float = 3.00

    # -- Grabador de la Fase 0 (§6) --------------------------------------------------
    data_dir: Path = REPO_ROOT / "data"
    """Parquet por hora y journal de épocas. No versionado (§4)."""

    state_db_path: Path = REPO_ROOT / "data" / "state.db"
    """SQLite en modo WAL con el estado vivo. El dashboard lo abre en SOLO LECTURA."""

    recorder_tick_s: float = 1.0
    """Una fila por segundo. Misma resolución que el experimento de §8 exige."""

    recorder_flush_s: float = 60.0
    """Cada cuánto se vuelca el buffer a disco. Un kill duro cuesta como mucho esto."""

    twap_window_hypotheses_s: tuple[float, ...] = (60.0, 900.0)
    """**Las dos lecturas de la ventana TWAP que §8 deja abiertas, grabadas a la vez.**

    La descripción del mercado dice que resuelve por «el TWAP del rango de tiempo
    especificado en el título» —900 s— pero cita como fuente el stream `btc-usd-twap-60s`.
    Medido con `delta_per_dollar` a τ=60 s hay un factor 15 entre ambas, del mismo orden
    que el error que corrigió la v2: no se elige a ojo.

    Grabar las dos cuesta unas columnas y evita el único desenlace que el plan no puede
    permitirse, que es descubrir al final de la Fase 0 que los 14 días se grabaron con la
    hipótesis equivocada. La decisión se toma después, contra la resolución de cada época.
    """

    epoch_discovery_poll_s: float = 10.0
    """Sondeo de la época siguiente. El hueco entre épocas es de ~2 min (§8.1)."""

    epoch_resolution_poll_s: float = 60.0
    epoch_resolution_deadline_s: float = 7200.0
    """Tras esto se deja de preguntar por el desenlace de una época y se marca abandonada."""

    # -- Dashboard (proceso aparte, solo lectura) ------------------------------------
    dashboard_host: str = "127.0.0.1"
    dashboard_port: int = 8000
    dashboard_history_hours: int = 6
    """Ventana de la serie de basis del panel de la pregunta 5 de la Fase 0."""

    dashboard_histogram_hours: int = 24
    """Ventana del histograma de |fair − mid| (preguntas 2 y 6 de la Fase 0)."""

    # -- Ejecución ------------------------------------------------------------------
    default_tick_size: Decimal = Field(default=Decimal("0.01"))
    """Arranque; el tick real llega por `tick_size_change` en vivo (§4).

    `0.01` es el `minimum_tick_size` observado en la familia `btc-updown-15m` el
    2026-09-08, no un valor de catálogo. Ojo: la tabla de §2.3 está expresada en ticks de
    `0.001`, así que sobre este mercado sus cifras de ticks se dividen por diez.
    """
