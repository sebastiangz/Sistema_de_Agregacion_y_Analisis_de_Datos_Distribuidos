from typing import Callable, List, TypeVar, Iterable, Tuple, Any
from itertools import islice

T = TypeVar("T")
K = TypeVar("K")

def partition(data: Iterable[T], n: int) -> List[List[T]]:
    data_list = list(data)
    if n <= 0:
        raise ValueError("n must be > 0")
    chunk_size = len(data_list) // n
    remainder = len(data_list) % n
    chunks: List[List[T]] = []
    start = 0
    for i in range(n):
        size = chunk_size + (1 if i < remainder else 0)
        chunks.append(data_list[start:start + size])
        start += size
    return chunks

def partition_by_key(data: Iterable[Tuple[K, Any]], key_fn: Callable[[K], int], n: int) -> List[List[Tuple[K, Any]]]:
    partitions: List[List[Tuple[K, Any]]] = [[] for _ in range(n)]
    for item in data:
        key, value = item
        pid = key_fn(key) % n
        partitions[pid].append(item)
    return partitions

def partition_by_range(data: Iterable[T], key_fn: Callable[[T], Any], ranges: List[Tuple[Any, Any]]) -> List[List[T]]:
    partitions = [[] for _ in range(len(ranges) + 1)]
    for item in data:
        v = key_fn(item)
        placed = False
        for i, (start, end) in enumerate(ranges):
            if start <= v < end:
                partitions[i].append(item)
                placed = True
                break
        if not placed:
            partitions[-1].append(item)
    return partitions

