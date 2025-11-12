from typing import Callable, Iterable, TypeVar, List, Tuple, Any, Optional
from functools import reduce
from itertools import groupby
from multiprocessing import Pool, cpu_count
import multiprocessing
import logging

K = TypeVar("K")
V = TypeVar("V")
T = TypeVar("T")

logger = logging.getLogger(__name__)

class MapReduce:
    """
    Implementación funcional de MapReduce con soporte para:
    - map, flat_map, filter, reduce_by_key, sort_by_value, collect
    - carga de datos y ejecución con multiprocessing (local) o modo secuencial
    - permite usar funciones puras; no internaliza estado mutable fuera de `self.data`
    """

    def __init__(self, workers: Optional[int] = None, use_processes: bool = True):
        self.workers = workers or max(1, cpu_count() - 1)
        self.use_processes = use_processes
        self.data: List[Any] = []

    # Utilities
    def load(self, data: Iterable[T]) -> "MapReduce":
        self.data = list(data)
        return self

    def _parallel_map(self, fn: Callable, data: List[Any]) -> List[Any]:
        if not self.use_processes or self.workers == 1:
            return list(map(fn, data))
        with Pool(self.workers) as pool:
            return pool.map(fn, data)

    # Core operations
    def map(self, mapper: Callable[[T], Any]) -> "MapReduce":
        self.data = self._parallel_map(mapper, self.data)
        return self

    def flat_map(self, mapper: Callable[[T], Iterable[Any]]) -> "MapReduce":
        mapped = self._parallel_map(mapper, self.data)
        # aplanar
        flattened = []
        for sub in mapped:
            if sub is None:
                continue
            for item in sub:
                flattened.append(item)
        self.data = flattened
        return self

    def filter(self, predicate: Callable[[T], bool]) -> "MapReduce":
        # Filter se ejecuta en el proceso principal para mantener orden determinista
        self.data = list(filter(predicate, self.data))
        return self

    def reduce_by_key(self, reducer: Callable[[V, V], V]) -> "MapReduce":
        # Se espera que self.data sea Iterable[Tuple[K, V]]
        # ordenar por clave para agrupar
        sorted_data = sorted(self.data, key=lambda x: x[0])
        result: List[Tuple[K, V]] = []
        for key, group in groupby(sorted_data, key=lambda x: x[0]):
            values = [item[1] for item in group]
            if not values:
                continue
            reduced_value = reduce(reducer, values)
            result.append((key, reduced_value))
        self.data = result
        return self

    def group_by_key(self) -> "MapReduce":
        sorted_data = sorted(self.data, key=lambda x: x[0])
        result: List[Tuple[K, List[V]]] = []
        for key, group in groupby(sorted_data, key=lambda x: x[0]):
            values = [item[1] for item in group]
            result.append((key, values))
        self.data = result
        return self

    def sort_by_value(self, key_fn: Callable[[Any], Any] = lambda x: x[1], descending: bool = False) -> "MapReduce":
        self.data = sorted(self.data, key=key_fn, reverse=descending)
        return self

    def collect(self) -> List[Any]:
        return self.data

    # Convenience: local combine to reduce network / IPC cost
    def combine_local(self, combiner_fn: Callable[[Any, Any], Any]) -> "MapReduce":
        """
        Ejecuta combiners locales: agrupa por clave en el nodo actual y combina valores.
        Útil antes de shuffle en escenarios distribuidos.
        """
        if not self.data:
            return self
        # agrupamiento local
        d = {}
        for k, v in self.data:
            if k in d:
                d[k] = combiner_fn(d[k], v)
            else:
                d[k] = v
        self.data = list(d.items())
        return self

    # Join simple (inner join) por clave con otro dataset
    def join(self, other: Iterable[Tuple[K, V]]) -> "MapReduce":
        # materializar en diccionario (útil para conjuntos pequeños)
        other_map = {}
        for k, v in other:
            other_map.setdefault(k, []).append(v)
        joined = []
        for k, v in self.data:
            if k in other_map:
                for ov in other_map[k]:
                    joined.append((k, (v, ov)))
        self.data = joined
        return self

    # Debug / inspect
    def take(self, n: int = 10) -> List[Any]:
        return self.data[:n]

