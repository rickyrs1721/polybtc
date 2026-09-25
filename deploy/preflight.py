"""Decide si una máquina candidata sirve, ANTES de comprometerse con ella.

Se ejecuta EN el VPS recién creado, con el plazo más corto posible todavía. Cuesta unos
céntimos y responde las tres preguntas que, contestadas tarde, cuestan 14 días o una mudanza:

1. **¿Desde qué país se ve esta IP, y qué permite ese país?** Grabar no es operar, así que
   la Fase 0 funciona casi en cualquier sitio; pero si el país está en `CLOSE_ONLY_API`, en
   el Sprint 7 el bot no podrá abrir posiciones y habrá que mudarse — y mudarse cambia el
   perfil de latencia con el que se midieron los 14 días.

2. **¿Cuánto edge fantasma fabrica esta ubicación?** El `fair` sale de Binance y el `mid`
   del libro de Polymarket. Lo que contamina `|fair − mid|` no es la latencia de ninguno de
   los dos, es **la diferencia entre ambas**: si el libro llega más viejo que el spot, se ve
   movimiento que ya no existe. Aquí se mide esa diferencia y se traduce a céntimos de fair
   con el propio modelo del proyecto, que es la única unidad en la que la cifra significa
   algo contra el colchón de 3¢ de §3.5.

3. **¿Aguantan los feeds?** Una hora de RTDS y Binance contando reconexiones, episodios de
   staleness y la divergencia entre el Binance directo y el relayado por Polymarket, que es
   el cross-check gratis de §2.1.b.

Los umbrales están escritos **antes** de correrlo, a propósito: decidirlos después de ver
los números es cómo se justifica cualquier cosa (§6).

Uso:
    uv run python -m deploy.preflight                 # una hora de feeds
    uv run python -m deploy.preflight --minutes 5     # prueba rápida
    uv run python -m deploy.preflight --skip-feeds    # solo geobloqueo, reloj y latencia
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import math
import socket
import statistics
import sys
import time
import urllib.request
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from urllib.parse import urlparse

from deploy.geoblock import GEOBLOCK_URL, Standing, standing_for
from polybtc.config import Settings
from polybtc.feeds import rtds, spot
from polybtc.feeds.state import (
    FEED_BINANCE_DIRECT,
    FEED_BINANCE_RELAY,
    FEED_CHAINLINK,
    FEED_TWAP30,
    FEED_TWAP60,
    FeedState,
)
from polybtc.model.binary import delta_per_dollar
from polybtc.model.twap import terminal_variance
from polybtc.model.vol import deannualize

# -- Umbrales, escritos antes de mirar ningún resultado --------------------------------

CLOCK_TOLERANCE_S = 2.0
"""Más desviación que esta y el halt por staleness de §5.2 miente: mide con reloj de pared."""

RTT_WARN_MS = 100.0
RTT_FAIL_MS = 250.0

PHANTOM_WARN_CENTS = 1.0
PHANTOM_FAIL_CENTS = 3.0
"""3¢ es el colchón de modelo de §3.5: una ubicación que fabrica eso por sí sola no sirve."""

RECONNECTS_WARN_PER_HOUR = 6.0
RECONNECTS_FAIL_PER_HOUR = 20.0

STALE_WARN_SHARE = 0.01
STALE_FAIL_SHARE = 0.05

WARMUP_S = 60.0
"""Segundos iniciales que NO cuentan para la estadística de staleness.

Al arrancar, ningún feed tiene datos todavía y `age_s` devuelve `None`, que `is_stale`
trata como stale — correctamente, porque «no hay dato» no es «fresco». Pero eso es el
calentamiento, no un problema de red: sin excluirlo, una corrida de dos minutos avisa
siempre y el aviso no significa nada.
"""

# -- Parámetros de la traducción a céntimos --------------------------------------------

REFERENCE_SIGMA_ANNUAL = 0.60
REFERENCE_TAU_S = 60.0
REFERENCE_WINDOW_S = 60.0
"""σ=60%, τ=60 s, w=60 s: el mismo punto de referencia que usa §2.1.c para el basis."""

RTT_SAMPLES = 12
SAMPLE_INTERVAL_S = 1.0

SPOT_LABEL = "spot del modelo"
"""Etiqueta del endpoint que alimenta al modelo. El diferencial se mide contra ESTE.

Medirlo siempre contra Binance dejaría el preflight fallando para siempre por un feed que
quizá ni se use: desde Europa, Binance está a ~270 ms (Tokio) y Coinbase a ~8 ms, y cuál de
los dos manda es `Settings.spot_source`.
"""


def endpoints(settings: Settings) -> tuple[tuple[str, str], ...]:
    """Endpoints a medir. El del spot depende de la fuente configurada."""
    spot_url = (
        settings.coinbase_ws_url
        if settings.spot_source == "coinbase"
        else f"{settings.binance_ws_base_url}/{settings.binance_symbol}@bookTicker"
    )
    other = (
        ("Binance (no usado)", f"{settings.binance_ws_base_url}/x")
        if settings.spot_source == "coinbase"
        else ("Coinbase (no usado)", settings.coinbase_ws_url)
    )
    return (
        ("RTDS (Polymarket)", "wss://ws-live-data.polymarket.com"),
        ("CLOB market (Polymarket)", "wss://ws-subscriptions-clob.polymarket.com/ws/market"),
        ("Gamma (Polymarket)", "https://gamma-api.polymarket.com"),
        (f"{SPOT_LABEL} · {settings.spot_source}", spot_url),
        other,
    )


class Verdict(StrEnum):
    PASS = "OK"
    WARN = "AVISO"
    FAIL = "FALLA"

    @property
    def rank(self) -> int:
        return {Verdict.PASS: 0, Verdict.WARN: 1, Verdict.FAIL: 2}[self]


@dataclass(frozen=True, slots=True)
class Check:
    name: str
    verdict: Verdict
    detail: str

    def render(self) -> str:
        mark = {Verdict.PASS: "·", Verdict.WARN: "!", Verdict.FAIL: "X"}[self.verdict]
        return f" {mark} {self.verdict.value:<6} {self.name:<34} {self.detail}"


# -- Utilidades HTTP ---------------------------------------------------------------------

USER_AGENT = "polybtc-preflight/1.0 (+https://github.com/rickyrs1721/polybtc)"
"""Sin `User-Agent` propio, el borde de Cloudflare responde 403 al agente de `urllib`.

Comprobado: las dos consultas HTTP de este script fallaban con «HTTP Error 403: Forbidden»
y parecían un geobloqueo. Un preflight que confunde «me han bloqueado» con «no me he
presentado» descartaría una máquina perfectamente buena.
"""


def _request(url: str, method: str = "GET") -> urllib.request.Request:
    return urllib.request.Request(url, method=method, headers={"User-Agent": USER_AGENT})


# -- 1. Geobloqueo ----------------------------------------------------------------------


def check_geoblock() -> Check:
    """Dónde cree Polymarket que estás, y qué te deja hacer eso.

    No se mira el campo `blocked` de la respuesta: es la política del **frontend** y sale
    `true` en países desde los que la API sí funciona. Se enruta por `country` contra las
    tres listas de `deploy/geoblock.py`.
    """
    try:
        with urllib.request.urlopen(_request(GEOBLOCK_URL), timeout=15) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception as error:
        return Check("Geobloqueo", Verdict.FAIL, f"no se pudo consultar: {error}")

    country = str(payload.get("country", "") or "")
    region = str(payload.get("region", "") or "")
    ip = str(payload.get("ip", "") or "?")
    standing = standing_for(country, region)
    where = f"{country}{'-' + region if region else ''} ({ip})"

    if standing is Standing.BLOCKED:
        return Check("Geobloqueo", Verdict.FAIL, f"{where}: bloqueado por completo")
    if standing is Standing.CLOSE_ONLY_API:
        return Check(
            "Geobloqueo",
            Verdict.WARN,
            f"{where}: {standing.value}. Sirve para la Fase 0, NO para el Sprint 7: "
            "desde aquí el bot no podrá abrir posiciones",
        )
    return Check("Geobloqueo", Verdict.PASS, f"{where}: {standing.value}")


# -- 1.b Ruta IPv4 ------------------------------------------------------------------------

IPV4_ONLY_HOST = ("stream.binance.com", 9443)
"""Binance **no publica AAAA**: su stream es IPv4 puro (comprobado el 2026-09-18).

Importa porque varios proveedores venden la máquina más barata **sin IPv4**, cobrando aparte
la dirección pública. En una máquina IPv6-only, Polymarket funciona —sus endpoints son
dual-stack— y Binance no. Y eso no se cae con estrépito: el grabador arranca, RTDS emite, y
`binance_direct` simplemente no se actualiza nunca.

Sin Binance no hay spot, sin spot no hay σ (§3.4) ni corrección de basis (§2.1.b), y sin
basis no hay fair. Catorce días grabando precios de Chainlink y ninguna respuesta a la
pregunta 2 de §6. **La IPv4 no es opcional en este proyecto.**
"""


def check_ipv4() -> Check:
    """¿Llega esta máquina a un endpoint que solo existe en IPv4?"""
    host, port = IPV4_ONLY_HOST
    try:
        infos = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
    except socket.gaierror as error:
        return Check("Ruta IPv4", Verdict.FAIL, f"{host} no resuelve por IPv4: {error}")
    if not infos:
        return Check("Ruta IPv4", Verdict.FAIL, f"{host} sin direcciones IPv4")

    family, socktype, proto, _, address = infos[0]
    try:
        with socket.socket(family, socktype, proto) as sock:
            sock.settimeout(5)
            sock.connect(address)
    except OSError as error:
        return Check(
            "Ruta IPv4",
            Verdict.FAIL,
            f"sin salida IPv4 hacia {host} ({error}). Binance es IPv4 puro: en una máquina "
            "IPv6-only te quedas sin spot, sin σ y sin basis — o sea, sin fair",
        )
    return Check("Ruta IPv4", Verdict.PASS, f"salida IPv4 hacia {host} correcta")


# -- 2. Reloj ---------------------------------------------------------------------------


def check_clock() -> Check:
    """Desviación del reloj de pared contra la cabecera `Date` de un servidor público.

    Importa porque la edad de un feed se mide con el máximo entre el reloj monótono y el de
    pared (§5.2). Un reloj desviado no da error: da halts falsos, o peor, frescura falsa.
    """
    try:
        # GET y no HEAD: gamma-api responde 405 Method Not Allowed a HEAD (comprobado).
        request = _request("https://gamma-api.polymarket.com")
        before = time.time()
        with urllib.request.urlopen(request, timeout=10) as response:
            after = time.time()
            served = response.headers.get("Date")
    except Exception as error:
        return Check("Reloj", Verdict.WARN, f"no se pudo comprobar: {error}")

    if not served:
        return Check("Reloj", Verdict.WARN, "el servidor no mandó cabecera Date")

    remote = datetime.strptime(served, "%a, %d %b %Y %H:%M:%S %Z").replace(tzinfo=UTC)
    # La cabecera tiene resolución de un segundo, así que se compara contra el punto medio
    # de la petición y se descuenta esa granularidad antes de juzgar.
    offset = (before + after) / 2 - remote.timestamp()
    if abs(offset) > CLOCK_TOLERANCE_S:
        return Check(
            "Reloj",
            Verdict.FAIL,
            f"desviado {offset:+.1f} s. Activa chrony o systemd-timesyncd antes de grabar",
        )
    return Check("Reloj", Verdict.PASS, f"desviación {offset:+.1f} s")


# -- 3. Latencia y edge fantasma --------------------------------------------------------


def tcp_rtt_ms(host: str, port: int, samples: int = RTT_SAMPLES) -> float | None:
    """Mediana del tiempo de establecimiento TCP. Sin TLS: mide la red, no el handshake."""
    timings: list[float] = []
    for _ in range(samples):
        start = time.perf_counter()
        try:
            with socket.create_connection((host, port), timeout=5):
                timings.append((time.perf_counter() - start) * 1000.0)
        except OSError:
            continue
    return statistics.median(timings) if timings else None


def measure_endpoints(settings: Settings) -> tuple[list[Check], dict[str, float]]:
    """RTT a cada endpoint. Devuelve también las medianas para el cálculo del diferencial."""
    checks: list[Check] = []
    rtts: dict[str, float] = {}

    for label, url in endpoints(settings):
        parsed = urlparse(url)
        host = parsed.hostname or ""
        port = parsed.port or (443 if parsed.scheme in ("https", "wss") else 80)
        rtt = tcp_rtt_ms(host, port)

        if rtt is None:
            checks.append(Check(f"Alcance · {label}", Verdict.FAIL, f"{host}:{port} inalcanzable"))
            continue
        rtts[label] = rtt
        if "no usado" in label:
            # Se mide para poder comparar, pero no puede tumbar el veredicto.
            checks.append(Check(f"Alcance · {label}", Verdict.PASS, f"{rtt:.0f} ms"))
            continue
        verdict = (
            Verdict.FAIL
            if rtt > RTT_FAIL_MS
            else Verdict.WARN
            if rtt > RTT_WARN_MS
            else Verdict.PASS
        )
        checks.append(
            Check(f"Alcance · {label}", verdict, f"{rtt:.0f} ms (mediana de {RTT_SAMPLES})")
        )

    return checks, rtts


def phantom_edge_cents(lag_s: float, spot: float = 78_500.0) -> float:
    """Céntimos de fair que fabrica por sí solo un desfase de `lag_s` entre los dos feeds.

    Si el libro llega `lag_s` más viejo que el spot, BTC se ha movido `σ·S·√lag` entretanto
    y esa diferencia entra entera en `|fair − mid|` sin que haya ninguna oportunidad. Es el
    mismo mecanismo que el basis de §2.1.b: una magnitud espuria que fabrica exactamente la
    conclusión «hay edge» con la que se gastaría el capital.
    """
    sigma_s = deannualize(REFERENCE_SIGMA_ANNUAL)
    drift = sigma_s * spot * math.sqrt(lag_s)
    variance = terminal_variance(sigma_s, REFERENCE_TAU_S, REFERENCE_WINDOW_S)
    return delta_per_dollar(spot, spot, variance) * drift * 100.0


def check_differential(rtts: dict[str, float], settings: Settings) -> Check:
    """La DIFERENCIA de latencia, no la latencia. Es un indicio fuerte, no una medición fina.

    **El sesgo, que hay que conocer antes de creerse el número**: los endpoints de
    Polymarket están detrás de una CDN, así que el saludo TCP termina en el nodo de borde
    más cercano y no en el servidor real. Medido desde Perú: 59 ms a Polymarket y 246 ms a
    Binance, cuando Londres está a ~180 ms de ahí. El RTT a un host con CDN mide la
    distancia al borde, no al mercado, y por eso este diferencial puede exagerar.

    Sirve igual para lo que se usa —**comparar dos máquinas candidatas**, donde el sesgo es
    el mismo en las dos— pero la medición que manda es la del run de feeds: ahí el precio
    recorre el camino entero, borde incluido.
    """
    polymarket = rtts.get("CLOB market (Polymarket)")
    spot = rtts.get(f"{SPOT_LABEL} · {settings.spot_source}")
    if polymarket is None or spot is None:
        return Check("Edge fantasma por latencia", Verdict.WARN, "faltan medidas de RTT")

    # Media onda: un mensaje viaja en un sentido, no ida y vuelta.
    lag_s = abs(polymarket - spot) / 2.0 / 1000.0
    cents = phantom_edge_cents(lag_s)
    verdict = (
        Verdict.FAIL
        if cents > PHANTOM_FAIL_CENTS
        else Verdict.WARN
        if cents > PHANTOM_WARN_CENTS
        else Verdict.PASS
    )
    return Check(
        "Edge fantasma por latencia",
        verdict,
        f"|Polymarket − {settings.spot_source}| = {abs(polymarket - spot):.0f} ms → "
        f"{cents:.2f}¢ a τ=60 s (colchón de §3.5: {PHANTOM_FAIL_CENTS:.0f}¢). "
        "Indicio con sesgo de CDN: compara máquinas, no lo tomes como medida absoluta",
    )


# -- 4. Los feeds, de verdad ------------------------------------------------------------


@dataclass
class FeedSample:
    stale: int = 0
    total: int = 0
    relay_minus_direct: list[float] = field(default_factory=list)


async def run_feeds(minutes: float, settings: Settings) -> tuple[list[Check], FeedState]:
    """Levanta RTDS y Binance el tiempo pedido y resume cómo se han portado."""
    state = FeedState(staleness_s=settings.staleness_s)
    tasks = [
        asyncio.create_task(rtds.run(state, settings), name="rtds"),
        asyncio.create_task(spot.run(state, settings), name="spot"),
    ]
    sample = FeedSample()
    required = (FEED_CHAINLINK, FEED_TWAP30, FEED_TWAP60, FEED_BINANCE_DIRECT)
    started = time.monotonic()
    deadline = started + minutes * 60.0

    try:
        while time.monotonic() < deadline:
            await asyncio.sleep(SAMPLE_INTERVAL_S)
            warming = time.monotonic() < started + WARMUP_S
            if not warming:
                sample.total += 1
                if state.stale_feeds(required):
                    sample.stale += 1
            direct = state.prices.get(FEED_BINANCE_DIRECT)
            relay = state.prices.get(FEED_BINANCE_RELAY)
            if direct is not None and relay is not None:
                sample.relay_minus_direct.append(float(relay - direct))
            _progress(deadline)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    return _summarize_feeds(sample, state, minutes), state


def _progress(deadline: float) -> None:
    """Solo en terminal: por una tubería, el retorno de carro no borra y ensucia el informe."""
    if not sys.stdout.isatty():
        return
    remaining = max(0.0, deadline - time.monotonic())
    sys.stdout.write(f"\r   midiendo feeds… quedan {remaining / 60:5.1f} min ")
    sys.stdout.flush()


def _summarize_feeds(sample: FeedSample, state: FeedState, minutes: float) -> list[Check]:
    if sys.stdout.isatty():
        sys.stdout.write("\r" + " " * 48 + "\r")
    checks: list[Check] = []

    reconnects = sum(state.reconnects.values())
    per_hour = reconnects / max(minutes / 60.0, 1e-9)
    verdict = (
        Verdict.FAIL
        if per_hour > RECONNECTS_FAIL_PER_HOUR
        else Verdict.WARN
        if per_hour > RECONNECTS_WARN_PER_HOUR
        else Verdict.PASS
    )
    checks.append(
        Check(
            "Feeds · reconexiones", verdict, f"{reconnects} en {minutes:.0f} min ({per_hour:.1f}/h)"
        )
    )

    share = sample.stale / sample.total if sample.total else 1.0
    measured_min = max(minutes - WARMUP_S / 60.0, 0.0)
    verdict = (
        Verdict.FAIL
        if share > STALE_FAIL_SHARE
        else Verdict.WARN
        if share > STALE_WARN_SHARE
        else Verdict.PASS
    )
    checks.append(
        Check(
            "Feeds · staleness",
            verdict,
            f"{share * 100:.2f}% de las muestras con algún feed por encima del umbral "
            f"(sobre {measured_min:.0f} min, sin contar el calentamiento)",
        )
    )

    # El cross-check gratis de §2.1.b: el mismo Binance por dos caminos. Su dispersión mide
    # cuánto se separan los dos relojes desde AQUÍ, incluida la cadencia de ~1 s del relay.
    diffs = sample.relay_minus_direct
    if len(diffs) < 30:
        checks.append(Check("Feeds · relay vs directo", Verdict.WARN, "muestras insuficientes"))
    else:
        mean = statistics.fmean(diffs)
        dispersion = statistics.pstdev(diffs)
        checks.append(
            Check(
                "Feeds · relay vs directo",
                Verdict.PASS,
                f"media ${mean:+.2f}, desviación ${dispersion:.2f} "
                f"(§2.1.b: si diverge, el problema es la red, no una oportunidad)",
            )
        )

    return checks


# -- Orquestación ------------------------------------------------------------------------


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--minutes", type=float, default=60.0, help="duración de la prueba de feeds"
    )
    parser.add_argument("--skip-feeds", action="store_true", help="solo las comprobaciones rápidas")
    args = parser.parse_args()

    settings = Settings()
    print(f"preflight · {datetime.now(UTC).isoformat(timespec='seconds')}\n")

    checks = [check_geoblock(), check_ipv4(), check_clock()]
    endpoint_checks, rtts = measure_endpoints(settings)
    checks += endpoint_checks
    checks.append(check_differential(rtts, settings))

    for check in checks:
        print(check.render())

    if not args.skip_feeds:
        print(f"\n   {args.minutes:.0f} min de feeds en vivo…")
        feed_checks, _ = await run_feeds(args.minutes, settings)
        checks += feed_checks
        for check in feed_checks:
            print(check.render())

    worst = max(checks, key=lambda c: c.verdict.rank).verdict
    print(f"\n   veredicto: {worst.value}")
    if worst is Verdict.FAIL:
        print("   No montes la Fase 0 aquí. Prueba otra región antes de comprometer el plazo.")
    elif worst is Verdict.WARN:
        print("   Revisa los avisos: puede servir para grabar y no para operar (Sprint 7).")
    return {Verdict.PASS: 0, Verdict.WARN: 0, Verdict.FAIL: 1}[worst]


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        raise SystemExit(asyncio.run(main()))
    raise SystemExit(130)
