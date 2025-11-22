from dataclasses import dataclass
from typing import Callable, Any, Iterable, Dict


@dataclass(frozen=True)
class Aggregator:
    """
    Agregador funcional: se puede reutilizar y componer.
    """
    initializer: Callable[[], Any]
    accumulator: Callable[[Any, Any], Any]
    combiner: Callable[[Any, Any], Any]
    finalizer: Callable[[Any], Any] = lambda x: x

    def aggregate(self, data: Iterable[Any]) -> Any:
        acc = self.initializer()
        for item in data:
            acc = self.accumulator(acc, item)
        return self.finalizer(acc)

    def map(self, fn: Callable[[Any], Any]) -> "Aggregator":
        return Aggregator(
            initializer=self.initializer,
            accumulator=self.accumulator,
            combiner=self.combiner,
            finalizer=lambda x: fn(self.finalizer(x)),
        )


def sum_aggregator(field: str | None = None) -> Aggregator:
    return Aggregator(
        initializer=lambda: 0.0,
        accumulator=lambda acc, x: acc + (x[field] if field else x),
        combiner=lambda a, b: a + b,
    )


def count_aggregator() -> Aggregator:
    return Aggregator(
        initializer=lambda: 0,
        accumulator=lambda acc, _: acc + 1,
        combiner=lambda a, b: a + b,
    )


def avg_aggregator(field: str | None = None) -> Aggregator:
    return Aggregator(
        initializer=lambda: (0.0, 0),
        accumulator=lambda acc, x: (
            acc[0] + (x[field] if field else x),
            acc[1] + 1,
        ),
        combiner=lambda a, b: (a[0] + b[0], a[1] + b[1]),
        finalizer=lambda x: x[0] / x[1] if x[1] else 0.0,
    )


def compose_aggregators(**aggs: Aggregator):
    """
    Ejecuta varios agregadores sobre la misma colección.
    Devuelve diccionario {nombre: resultado}.
    """
    def combined(data: Iterable[Any]) -> Dict[str, Any]:
        data_list = list(data)
        return {name: agg.aggregate(data_list) for name, agg in aggs.items()}

    return combined

