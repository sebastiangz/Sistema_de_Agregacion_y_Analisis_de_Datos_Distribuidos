from dataclasses import dataclass
from typing import Callable, Any, Iterable, Dict, Tuple
from functools import reduce

@dataclass(frozen=True)
class Aggregator:
    """
    Agregador funcional composable:
      - initializer: -> estado inicial
      - accumulator: (estado, elemento) -> estado
      - combiner: (estado, estado) -> estado (para combinar sub-aggregados)
      - finalizer: estado -> resultado final
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

    def combine(self, a: Any, b: Any) -> Any:
        return self.combiner(a, b)

    def map(self, fn: Callable[[Any], Any]) -> "Aggregator":
        return Aggregator(
            initializer=self.initializer,
            accumulator=self.accumulator,
            combiner=self.combiner,
            finalizer=lambda x: fn(self.finalizer(x)),
        )


# Predefinidos

def sum_aggregator(field: str | None = None) -> Aggregator:
    return Aggregator(
        initializer=lambda: 0,
        accumulator=lambda acc, x: acc + (x[field] if field and isinstance(x, dict) else (getattr(x, field) if field and hasattr(x, field) else (x if field is None else 0))),
        combiner=lambda a, b: a + b,
        finalizer=lambda x: x
    )

def count_aggregator() -> Aggregator:
    return Aggregator(
        initializer=lambda: 0,
        accumulator=lambda acc, x: acc + 1,
        combiner=lambda a, b: a + b,
        finalizer=lambda x: x
    )

def avg_aggregator(field: str | None = None) -> Aggregator:
    # estado: (sum, count)
    return Aggregator(
        initializer=lambda: (0.0, 0),
        accumulator=lambda acc, x: (acc[0] + (x[field] if field and isinstance(x, dict) else (getattr(x, field) if field and hasattr(x, field) else (x if field is None else 0))), acc[1] + 1),
        combiner=lambda a, b: (a[0] + b[0], a[1] + b[1]),
        finalizer=lambda s: (s[0] / s[1]) if s[1] > 0 else 0.0
    )

def min_aggregator(field: str | None = None) -> Aggregator:
    return Aggregator(
        initializer=lambda: None,
        accumulator=lambda acc, x: (x[field] if field and isinstance(x, dict) else (getattr(x, field) if field and hasattr(x, field) else x)) if acc is None else min(acc, (x[field] if field and isinstance(x, dict) else (getattr(x, field) if field and hasattr(x, field) else x))),
        combiner=lambda a, b: a if b is None else (b if a is None else min(a, b)),
        finalizer=lambda x: x
    )

def max_aggregator(field: str | None = None) -> Aggregator:
    return Aggregator(
        initializer=lambda: None,
        accumulator=lambda acc, x: (x[field] if field and isinstance(x, dict) else (getattr(x, field) if field and hasattr(x, field) else x)) if acc is None else max(acc, (x[field] if field and isinstance(x, dict) else (getattr(x, field) if field and hasattr(x, field) else x))),
        combiner=lambda a, b: a if b is None else (b if a is None else max(a, b)),
        finalizer=lambda x: x
    )

def compose_aggregators(**aggregators) -> Callable[[Iterable[Any]], Dict[str, Any]]:
    """
    Devuelve una función que aplica múltiples agregadores sobre el mismo iterable.
    Nota: consume el iterable si es un generator; mejor pasar listas o particiones.
    """
    def combined(data: Iterable[Any]) -> Dict[str, Any]:
        # si data es generador y se usa varias veces, materializar en lista
        data_list = list(data)
        return {name: agg.aggregate(data_list) for name, agg in aggregators.items()}
    return combined

