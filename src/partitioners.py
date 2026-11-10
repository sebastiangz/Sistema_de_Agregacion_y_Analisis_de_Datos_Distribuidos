"""
Estrategias de particionamiento (funciones puras).

Todas devuelven tuplas de tuplas: la entrada nunca se modifica y las
particiones resultantes son inmutables.
"""
from __future__ import annotations

import zlib
from typing import Any, Callable, Hashable, Iterable, List, Sequence, Tuple, TypeVar

T = TypeVar("T")
K = TypeVar("K", bound=Hashable)
V = TypeVar("V")

Partitions = Tuple[Tuple[T, ...], ...]


def chunk(data: Iterable[T], n: int) -> Partitions:
    """Divide los datos en ``n`` bloques contiguos de tamaño parecido.

    >>> chunk(range(7), 3)
    ((0, 1, 2), (3, 4), (5, 6))
    """
    if n < 1:
        raise ValueError("n debe ser >= 1")
    items = tuple(data)
    base, extra = divmod(len(items), n)
    bounds = []
    start = 0
    for i in range(n):
        end = start + base + (1 if i < extra else 0)
        bounds.append((start, end))
        start = end
    return tuple(items[s:e] for s, e in bounds)


def stable_hash(key: Any) -> int:
    """Hash determinista entre procesos.

    ``hash()`` de Python cambia entre ejecuciones para str (PYTHONHASHSEED),
    así que para repartir claves entre workers usamos CRC32 de su repr.
    """
    return zlib.crc32(repr(key).encode("utf-8"))


def hash_partition(
    pairs: Iterable[Tuple[K, V]],
    n: int,
    hash_fn: Callable[[K], int] = stable_hash,
) -> Partitions:
    """Reparte pares (clave, valor) en ``n`` particiones según el hash de la clave.

    Garantía: todas las ocurrencias de una misma clave terminan en la misma
    partición (esto es el *shuffle* de Map-Reduce).
    """
    if n < 1:
        raise ValueError("n debe ser >= 1")
    buckets: list[list[Tuple[K, V]]] = [[] for _ in range(n)]
    for k, v in pairs:
        buckets[hash_fn(k) % n].append((k, v))
    return tuple(tuple(b) for b in buckets)


def range_partition(
    data: Iterable[T],
    key_fn: Callable[[T], Any],
    boundaries: Sequence[Any],
) -> Partitions:
    """Particiona por rangos ordenados.

    Con ``boundaries = [10, 20]`` se generan 3 particiones:
    ``(-inf, 10)``, ``[10, 20)`` y ``[20, +inf)``.
    """
    bounds = tuple(boundaries)
    if list(bounds) != sorted(bounds):
        raise ValueError("boundaries debe estar ordenado")
    buckets: list[list[T]] = [[] for _ in range(len(bounds) + 1)]
    for item in data:
        value = key_fn(item)
        idx = sum(1 for b in bounds if value >= b)
        buckets[idx].append(item)
    return tuple(tuple(b) for b in buckets)


# ---------------------------------------------------------------------------
# Compatibilidad: funciones originales de la base (devuelven listas).
# Se conservan sin cambios; el motor nuevo usa chunk / hash_partition.
# ---------------------------------------------------------------------------

def partition(data: Iterable[T], n: int) -> List[List[T]]:
    """
    Particiona datos en n bloques de tamaño parecido.
    """
    data_list = list(data)
    length = len(data_list)
    if n <= 0:
        return [data_list]

    base = length // n
    extra = length % n

    chunks: List[List[T]] = []
    start = 0
    for i in range(n):
        size = base + (1 if i < extra else 0)
        end = start + size
        chunks.append(data_list[start:end])
        start = end
    return chunks


def partition_by_key(
    data: Iterable[Tuple[K, V]],
    key_fn: Callable[[K], int],
    n: int,
) -> List[List[Tuple[K, V]]]:
    """
    Particiona según un hash sencillo de la clave.
    """
    parts: List[List[Tuple[K, V]]] = [[] for _ in range(max(n, 1))]
    for k, v in data:
        idx = key_fn(k) % max(n, 1)
        parts[idx].append((k, v))
    return parts


def partition_by_range(
    data: Iterable[T],
    key_fn: Callable[[T], Any],
    ranges: List[Tuple[Any, Any]],
) -> List[List[T]]:
    """
    Particiona en función de un rango de valores.
    """
    parts: List[List[T]] = [[] for _ in range(len(ranges) + 1)]
    for item in data:
        value = key_fn(item)
        placed = False
        for i, (start, end) in enumerate(ranges):
            if start <= value < end:
                parts[i].append(item)
                placed = True
                break
        if not placed:
            parts[-1].append(item)
    return parts

