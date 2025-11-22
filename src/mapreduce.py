from typing import Callable, Iterable, List, Tuple, TypeVar, Generic, Optional, Any, cast
from functools import reduce
from itertools import groupby
from multiprocessing import Pool, cpu_count

K = TypeVar("K")
V2 = TypeVar("V2")
T = TypeVar("T")

class MapReduce(Generic[K, V2]):
    """
    MapReduce funcionalmente tipado.
    Usar map y flat_map primero con datos de cualquier tipo,
    y luego cambiar a datos de tipo Tuple[K, V2] para reduce_by_key o sort.
    """

    def __init__(self, workers: Optional[int] = None, data: Optional[Iterable[Any]] = None):
        self.workers = workers or cpu_count()
        self.data: List[Any] = list(data) if data is not None else []

    def load(self, data: Iterable[Any]) -> "MapReduce[K, V2]":
        self.data = list(data)
        return self

    def map(self, mapper: Callable[[Any], Any]) -> "MapReduce[K, V2]":
        with Pool(self.workers) as pool:
            self.data = pool.map(mapper, self.data)
        return self

    def flat_map(self, mapper: Callable[[Any], Iterable[Any]]) -> "MapReduce[K, V2]":
        with Pool(self.workers) as pool:
            mapped = pool.map(mapper, self.data)
        self.data = [item for sublist in mapped for item in sublist]
        return self

    def filter(self, predicate: Callable[[Any], bool]) -> "MapReduce[K, V2]":
        self.data = [x for x in self.data if predicate(x)]
        return self

    def reduce_by_key(self, reducer: Callable[[V2, V2], V2]) -> "MapReduce[K, Tuple[K, V2]]":
        """
        Solo debe llamarse si self.data es List[Tuple[K, V2]].
        """
        if not self.data:
            return cast("MapReduce[K, Tuple[K, V2]]", self)

        tuple_data: List[Tuple[K, V2]] = cast(List[Tuple[K, V2]], self.data)
        sorted_data = sorted(tuple_data, key=lambda x: x[0])  # type: ignore
        result: List[Tuple[K, V2]] = []
        for key, group in groupby(sorted_data, key=lambda x: x[0]):  # type: ignore
            values = [item[1] for item in group]
            reduced_value = reduce(reducer, values)
            result.append((key, reduced_value))
        self.data = cast(List[Any], result)
        return cast("MapReduce[K, Tuple[K, V2]]", self)

    def sort_by_value(self, descending: bool = False) -> "MapReduce[K, V2]":
        ###type: ignore porque asume self.data es List[Tuple[K, V2]]
        self.data = sorted(self.data, key=lambda x: x[1], reverse=descending)  # type: ignore
        return self

    def sort_by_key(self, descending: bool = False) -> "MapReduce[K, V2]":
        ###type: ignore porque asume self.data es List[Tuple[K, V2]]
        self.data = sorted(self.data, key=lambda x: x[0], reverse=descending)  # type: ignore
        return self

    def collect(self) -> List[Any]:
        return list(self.data)

