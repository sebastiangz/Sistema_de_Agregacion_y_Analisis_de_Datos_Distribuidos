from typing import Iterable, List, TypeVar, Tuple, Any, Callable

T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")


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

