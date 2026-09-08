"""Escritura a Parquet particionado por hora, a prueba de un apagón a mitad de la Fase 0.

**Por qué no hay un `ParquetWriter` abierto.** Un Parquet solo es legible cuando se ha
escrito su footer, y el footer se escribe al cerrar. Un `ParquetWriter` abierto durante una
hora y un `kill -9` a mitad no dejan un archivo incompleto: dejan un archivo **ilegible**,
y como el análisis del Sprint 5 lee el directorio entero con `pyarrow.dataset`, un solo
archivo sin footer envenena la lectura de los 14 días. El plan ya avisa de lo que cuesta un
hueco en los datos: reiniciar el contador de la Fase 0.

Lo que se hace en su lugar, y es el diseño central de este módulo:

1. Cada volcado escribe un `RecordBatch` a un **stream Arrow IPC** `HH.arrow.part`, y se
   hace `fsync`. El formato IPC es una secuencia de bloques autocontenidos: si el proceso
   muere a mitad, lo escrito hasta el último bloque completo se sigue leyendo.
2. Al cambiar de hora —o al cerrar ordenadamente, o al arrancar y encontrar restos— el
   `.part` se convierte de una vez a `HH.parquet` con escritura atómica (tmp + `rename`).

Consecuencia: **todo `.parquet` bajo `data/` está siempre completo y es siempre legible.**
Un `kill -9` cuesta como mucho las filas del último minuto sin volcar, nunca una hora, y
nunca deja basura que rompa el análisis.

Estructura resultante, la de §6:

    data/2026/09/08/17.parquet     hora cerrada
    data/2026/09/08/18.arrow.part  hora en curso (el dashboard también la lee)
"""

from __future__ import annotations

import os
from collections.abc import Iterator, Sequence
from datetime import datetime
from pathlib import Path
from types import TracebackType
from typing import IO, Self

import pyarrow as pa
import pyarrow.parquet as pq

from polybtc.recorder.schema import Sample, build_schema, to_record_batch

PART_SUFFIX = ".arrow.part"
PARQUET_SUFFIX = ".parquet"
TMP_SUFFIX = ".tmp"


class ParquetSink:
    """Buffer en memoria → IPC por hora → Parquet al cerrar la hora.

    No es thread-safe ni falta: el grabador es un único bucle asíncrono y este objeto se
    toca solo desde él.
    """

    def __init__(self, root: Path, windows: Sequence[float]) -> None:
        self._root = root
        self._windows = tuple(windows)
        self._schema = build_schema(self._windows)
        self._buffer: list[Sample] = []
        self._hour_key: tuple[int, int, int, int] | None = None
        self._part_path: Path | None = None
        self._handle: IO[bytes] | None = None
        self._writer: pa.RecordBatchStreamWriter | None = None
        self._rows_written = 0

    # -- ciclo de vida -----------------------------------------------------------------

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def append(self, sample: Sample) -> None:
        """Encola una fila. No toca disco: eso pasa en `flush`."""
        self._buffer.append(sample)

    def flush(self) -> int:
        """Vuelca el buffer al `.part` de la hora y lo sincroniza a disco.

        Devuelve cuántas filas se escribieron. El `fsync` es deliberado: sin él, «escrito»
        significa «en el caché del sistema operativo», que es exactamente lo que un corte
        de luz se lleva por delante.
        """
        if not self._buffer:
            return 0

        # Un volcado puede cruzar el cambio de hora. Se parte por hora para que ninguna
        # fila acabe en el archivo equivocado: la partición es por hora de la muestra, no
        # por hora del volcado, y el análisis del Sprint 5 asume justo eso.
        written = 0
        for key, group in _group_by_hour(self._buffer):
            self._ensure_hour(key)
            batch = to_record_batch(group, self._windows)
            assert self._writer is not None
            self._writer.write_batch(batch)
            written += batch.num_rows

        assert self._handle is not None
        self._handle.flush()
        os.fsync(self._handle.fileno())

        self._buffer.clear()
        self._rows_written += written
        return written

    def close(self) -> None:
        """Vuelca lo pendiente y cierra la hora en curso convirtiéndola a Parquet.

        Es el camino de un `SIGTERM` ordenado: al volver, `data/` no tiene ningún `.part`
        pendiente y la última hora ya es un Parquet completo.
        """
        self.flush()
        self._finalize_hour()

    # -- estado, para el heartbeat -----------------------------------------------------

    @property
    def rows_written(self) -> int:
        return self._rows_written

    @property
    def pending(self) -> int:
        return len(self._buffer)

    @property
    def current_part(self) -> Path | None:
        return self._part_path

    # -- interno -----------------------------------------------------------------------

    def _ensure_hour(self, key: tuple[int, int, int, int]) -> None:
        if self._hour_key == key:
            return
        self._finalize_hour()

        year, month, day, hour = key
        directory = self._root / f"{year:04d}" / f"{month:02d}" / f"{day:02d}"
        directory.mkdir(parents=True, exist_ok=True)
        self._part_path = _free_path(directory, f"{hour:02d}", PART_SUFFIX)
        self._handle = self._part_path.open("wb")
        self._writer = pa.ipc.new_stream(self._handle, self._schema)
        self._hour_key = key

    def _finalize_hour(self) -> None:
        if self._writer is None or self._handle is None or self._part_path is None:
            return
        self._writer.close()
        self._handle.flush()
        os.fsync(self._handle.fileno())
        self._handle.close()
        part = self._part_path

        self._writer = None
        self._handle = None
        self._part_path = None
        self._hour_key = None

        finalize_part(part)


def finalize_part(part: Path) -> Path | None:
    """`HH.arrow.part` → `HH.parquet`, atómico, y borra el `.part`.

    Devuelve la ruta del Parquet escrito, o `None` si el `.part` no tenía ni un bloque
    completo (un proceso que murió justo después de crear el archivo). Un `.part` vacío se
    borra en vez de quedarse: si no, cada arranque tras un fallo dejaría un cadáver más.
    """
    batches = list(read_part(part))
    if not batches:
        part.unlink(missing_ok=True)
        return None

    table = pa.Table.from_batches(batches)
    stem = part.name[: -len(PART_SUFFIX)]
    target = _free_path(part.parent, stem, PARQUET_SUFFIX)
    tmp = target.with_suffix(target.suffix + TMP_SUFFIX)

    pq.write_table(table, tmp, compression="zstd")
    # `rename` sobre el mismo sistema de ficheros es atómico: o está el Parquet completo o
    # no está. Nunca hay un archivo a medias esperando a que el análisis tropiece con él.
    tmp.replace(target)
    part.unlink(missing_ok=True)
    return target


def read_part(part: Path) -> Iterator[pa.RecordBatch]:
    """Lee los bloques completos de un `.part`, incluso si el archivo está truncado.

    Un `.part` de una hora en curso está siendo escrito ahora mismo, y uno de un proceso
    muerto termina a mitad de un bloque. Los dos casos se leen igual: bloques hasta que
    algo falle, y lo que falle se descarta en silencio. Es la propiedad por la que este
    módulo usa IPC y no Parquet como almacén primario.
    """
    if not part.exists():
        return
    try:
        with part.open("rb") as handle:
            reader = pa.ipc.open_stream(handle)
            while True:
                try:
                    yield reader.read_next_batch()
                except StopIteration:
                    return
                except pa.ArrowInvalid:
                    return  # cola truncada: lo anterior es bueno y es lo que se conserva
    except (pa.ArrowInvalid, OSError):
        return  # ni siquiera la cabecera llegó a escribirse


def recover_orphans(root: Path) -> list[Path]:
    """Convierte a Parquet los `.part` que dejó una muerte no ordenada. Devuelve lo escrito.

    Se llama al arrancar. Sin esto, un `kill -9` deja una hora legible solo por quien sepa
    que existe el formato IPC, que es otra forma de perderla.
    """
    written: list[Path] = []
    if not root.exists():
        return written
    for part in sorted(root.rglob(f"*{PART_SUFFIX}")):
        target = finalize_part(part)
        if target is not None:
            written.append(target)
    return written


def _group_by_hour(
    samples: Sequence[Sample],
) -> Iterator[tuple[tuple[int, int, int, int], list[Sample]]]:
    """Agrupa filas consecutivas por hora UTC, conservando el orden de llegada."""
    current: tuple[int, int, int, int] | None = None
    group: list[Sample] = []
    for sample in samples:
        key = _hour_key(sample.ts)
        if key != current:
            if group and current is not None:
                yield current, group
            current, group = key, []
        group.append(sample)
    if group and current is not None:
        yield current, group


def _hour_key(ts: datetime) -> tuple[int, int, int, int]:
    return (ts.year, ts.month, ts.day, ts.hour)


def _free_path(directory: Path, stem: str, suffix: str) -> Path:
    """`HH.parquet`, o `HH.1.parquet` si ya existe.

    Colisiona en un caso real y previsible: el grabador se reinicia dentro de la misma
    hora, así que esa hora acaba en dos archivos. Se numeran en vez de sobrescribir —
    perder la primera mitad de una hora por un reinicio sería el mismo agujero que todo
    este módulo existe para evitar—, y `pyarrow.dataset` los lee juntos igualmente.
    """
    candidate = directory / f"{stem}{suffix}"
    index = 1
    while candidate.exists():
        candidate = directory / f"{stem}.{index}{suffix}"
        index += 1
    return candidate
