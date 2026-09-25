"""`state.db`: SQLite en modo WAL con el estado vivo. El grabador escribe, el dashboard lee.

WAL no es un detalle de rendimiento, es lo que hace posible la separación de procesos: un
lector puede leer mientras el escritor escribe, sin bloquearse ni bloquear. Y el dashboard
abre la base en **solo lectura** (`mode=ro`), que es la única forma de garantizar por
construcción lo que el plan exige — que un fallo del dashboard no pueda tocar los datos del
grabador. Un «acuérdate de hacer solo SELECT» no es una garantía.

Dos tablas, y la segunda no es un capricho:

- `status` — una única fila que se reescribe cada segundo. Es el heartbeat.
- `epochs` — una fila por época, que se **actualiza después** de cerrarse cuando llega su
  resolución. Ese dato es el que decide la ventana TWAP de §8, llega minutos u horas tarde
  y muta una fila ya escrita: un Parquet no hace eso, una base de datos sí. El registro
  inmutable de todos modos existe aparte, en el journal JSONL de `epochs.py`.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping, Sequence
from pathlib import Path
from types import TracebackType
from typing import Any, Self

STATUS_COLUMNS: tuple[tuple[str, str], ...] = (
    ("updated_at", "TEXT"),
    ("pid", "INTEGER"),
    ("started_at", "TEXT"),
    ("uptime_s", "REAL"),
    ("ticks", "INTEGER"),
    ("rows_written", "INTEGER"),
    ("rows_pending", "INTEGER"),
    ("last_flush_at", "TEXT"),
    ("current_part", "TEXT"),
    ("halted", "INTEGER"),
    ("halt_reasons", "TEXT"),
    ("reconnects", "INTEGER"),
    ("age_chainlink", "REAL"),
    ("age_twap30", "REAL"),
    ("age_twap60", "REAL"),
    ("age_binance_direct", "REAL"),
    ("age_binance_relay", "REAL"),
    ("age_coinbase", "REAL"),
    ("age_book_up", "REAL"),
    ("age_book_down", "REAL"),
    ("staleness_s", "REAL"),
    ("chainlink", "REAL"),
    ("twap30", "REAL"),
    ("twap60", "REAL"),
    ("binance_mid", "REAL"),
    ("binance_relay", "REAL"),
    ("coinbase_mid", "REAL"),
    ("basis_usd", "REAL"),
    ("basis_log", "REAL"),
    ("basis_ready", "INTEGER"),
    ("basis_samples", "INTEGER"),
    ("basis_clamped", "INTEGER"),
    ("spot_oracle", "REAL"),
    ("spot_source", "TEXT"),
    ("sigma_s", "REAL"),
    ("sigma_annual", "REAL"),
    ("sigma_ready", "INTEGER"),
    ("sigma_in_band", "INTEGER"),
    ("sigma_samples", "INTEGER"),
    ("sigma_floor_annual", "REAL"),
    ("sigma_ceiling_annual", "REAL"),
    ("epoch_slug", "TEXT"),
    ("epoch_end", "TEXT"),
    ("tau_s", "REAL"),
    ("strike", "REAL"),
    ("strike_source", "TEXT"),
    ("up_bid", "REAL"),
    ("up_ask", "REAL"),
    ("down_bid", "REAL"),
    ("down_ask", "REAL"),
    ("up_tick", "REAL"),
    ("down_tick", "REAL"),
    ("fairs_json", "TEXT"),
    ("windows_json", "TEXT"),
)
"""Columnas de `status`, con tipo explícito.

Columnas de verdad y no un solo blob JSON porque a las 3 de la mañana del día 9 de la Fase
0 la herramienta de diagnóstico es `sqlite3 data/state.db "select * from status"`, y eso
solo sirve si los nombres están ahí. Las dos excepciones (`fairs_json`, `windows_json`) son
las que dependen de cuántas hipótesis de ventana haya configuradas (§8): hacerlas columnas
obligaría a migrar el esquema al cerrar esa decisión.
"""

EPOCH_COLUMNS: tuple[tuple[str, str], ...] = (
    ("slug", "TEXT PRIMARY KEY"),
    ("condition_id", "TEXT"),
    ("question", "TEXT"),
    ("epoch_start", "TEXT"),
    ("epoch_end", "TEXT"),
    ("up_token", "TEXT"),
    ("down_token", "TEXT"),
    ("created_at", "TEXT"),
    ("created_ahead_s", "REAL"),
    ("tick_size", "REAL"),
    ("min_order_size", "REAL"),
    ("seconds_delay", "INTEGER"),
    ("strike_chainlink", "REAL"),
    ("strike_twap30", "REAL"),
    ("strike_twap60", "REAL"),
    ("strike_source", "TEXT"),
    ("strike_ts", "TEXT"),
    ("settle_chainlink", "REAL"),
    ("settle_twap30", "REAL"),
    ("settle_twap60", "REAL"),
    ("settle_ts", "TEXT"),
    ("resolved_outcome", "TEXT"),
    ("resolved_at", "TEXT"),
    ("resolution_attempts", "INTEGER"),
    ("discovered_at", "TEXT"),
    ("updated_at", "TEXT"),
)
"""Una fila por época. `strike_*` se rellena al abrir, `settle_*` al cerrar y
`resolved_outcome` cuando la plataforma publica el desenlace — que es el experimento de §8.
"""


class StatusStore:
    """Escritor de `state.db`. Solo el grabador instancia esto."""

    def __init__(self, path: Path) -> None:
        self._path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(path, isolation_level=None)
        self._conn.execute("PRAGMA journal_mode=WAL")
        # NORMAL y no FULL a propósito: `state.db` es estado VIVO y reconstruible: si se
        # pierde el último segundo tras un corte de luz no se pierde nada que importe. Los
        # datos que sí importan van por `sink.py`, que sí hace fsync en cada volcado.
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._create_tables()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        self._conn.close()

    def _create_tables(self) -> None:
        status_cols = ", ".join(f"{name} {sqltype}" for name, sqltype in STATUS_COLUMNS)
        self._conn.execute(
            f"CREATE TABLE IF NOT EXISTS status ("
            f"id INTEGER PRIMARY KEY CHECK (id = 1), {status_cols})"
        )
        epoch_cols = ", ".join(f"{name} {sqltype}" for name, sqltype in EPOCH_COLUMNS)
        self._conn.execute(f"CREATE TABLE IF NOT EXISTS epochs ({epoch_cols})")
        self._conn.execute("CREATE INDEX IF NOT EXISTS epochs_end ON epochs (epoch_end)")

    def write_status(self, values: Mapping[str, Any]) -> None:
        """Reescribe la fila única de `status`. Una columna ausente se guarda como NULL.

        Un `INSERT ... ON CONFLICT DO UPDATE` en vez de un `DELETE`+`INSERT`: el lector
        nunca ve la tabla vacía, ni siquiera durante un instante.
        """
        names = [name for name, _ in STATUS_COLUMNS]
        unknown = set(values) - set(names)
        if unknown:
            raise KeyError(f"columnas desconocidas en status: {sorted(unknown)}")

        placeholders = ", ".join("?" for _ in names)
        assignments = ", ".join(f"{name}=excluded.{name}" for name in names)
        row = [_encode(values.get(name)) for name in names]
        self._conn.execute(
            f"INSERT INTO status (id, {', '.join(names)}) VALUES (1, {placeholders}) "
            f"ON CONFLICT(id) DO UPDATE SET {assignments}",
            row,
        )

    def upsert_epoch(self, values: Mapping[str, Any]) -> None:
        """Crea o actualiza la fila de una época. Solo toca las columnas que se pasan.

        Actualizar parcialmente es el punto: la fila nace al descubrir la época, se
        completa con el strike al abrir, con los valores de cierre al vencer y con la
        resolución cuando llega, que puede ser horas después.
        """
        names = [name for name, _ in EPOCH_COLUMNS]
        unknown = set(values) - set(names)
        if unknown:
            raise KeyError(f"columnas desconocidas en epochs: {sorted(unknown)}")
        if "slug" not in values:
            raise KeyError("upsert_epoch necesita 'slug'")

        given = [name for name in names if name in values]
        placeholders = ", ".join("?" for _ in given)
        assignments = ", ".join(f"{name}=excluded.{name}" for name in given if name != "slug")
        row = [_encode(values[name]) for name in given]
        self._conn.execute(
            f"INSERT INTO epochs ({', '.join(given)}) VALUES ({placeholders}) "
            f"ON CONFLICT(slug) DO UPDATE SET {assignments}",
            row,
        )

    def pending_resolutions(self) -> list[dict[str, Any]]:
        """Épocas ya cerradas cuyo desenlace todavía no se ha podido leer."""
        cursor = self._conn.execute(
            "SELECT * FROM epochs WHERE resolved_outcome IS NULL ORDER BY epoch_end"
        )
        return _rows(cursor)


class StatusReader:
    """Lector de `state.db` en SOLO LECTURA. Es lo que usa el dashboard.

    `mode=ro` es una garantía del motor, no una convención: con esta conexión, un bug del
    dashboard no puede escribir en la base del grabador aunque lo intente.
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._conn: sqlite3.Connection | None = None

    def _connect(self) -> sqlite3.Connection | None:
        """Conecta perezosamente. Devuelve `None` mientras el grabador no haya arrancado.

        El dashboard tiene que poder arrancar ANTES que el grabador y sobrevivir a que se
        reinicie. Un fallo aquí es «todavía no hay datos», no un error de la aplicación.
        """
        if self._conn is not None:
            return self._conn
        if not self._path.exists():
            return None
        try:
            self._conn = sqlite3.connect(
                f"file:{self._path}?mode=ro", uri=True, check_same_thread=False
            )
        except sqlite3.Error:
            return None
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def status(self) -> dict[str, Any] | None:
        conn = self._connect()
        if conn is None:
            return None
        try:
            rows = _rows(conn.execute("SELECT * FROM status WHERE id = 1"))
        except sqlite3.Error:
            self.close()  # el grabador puede haber recreado el archivo: reconectar luego
            return None
        return rows[0] if rows else None

    def epochs(self, limit: int = 50) -> list[dict[str, Any]]:
        conn = self._connect()
        if conn is None:
            return []
        try:
            return _rows(
                conn.execute("SELECT * FROM epochs ORDER BY epoch_end DESC LIMIT ?", (limit,))
            )
        except sqlite3.Error:
            self.close()
            return []


def _rows(cursor: sqlite3.Cursor) -> list[dict[str, Any]]:
    names: Sequence[str] = [d[0] for d in cursor.description]
    return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def _encode(value: Any) -> Any:
    """SQLite no tiene booleano: se guardan como 0/1 para que el dashboard no adivine."""
    if isinstance(value, bool):
        return int(value)
    return value
