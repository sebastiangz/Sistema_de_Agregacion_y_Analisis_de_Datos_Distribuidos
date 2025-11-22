from typing import Iterable, Callable, Any, List, Tuple, Dict
from collections import defaultdict


def local_combine_pairs(
    pairs: Iterable[Tuple[Any, Any]],
    reduce_fn: Callable[[Any, Any], Any],
) -> List[Tuple[Any, Any]]:
    """
    Combina localmente pares (k, v) en memoria usando reduce_fn.
    Útil como combiner antes de un reduce global.
    """
    groups: Dict[Any, Any] = {}
    for k, v in pairs:
        if k in groups:
            groups[k] = reduce_fn(groups[k], v)
        else:
            groups[k] = v
    return list(groups.items())


def grouped_reduce(
    data: Iterable[Any],
    key_fn: Callable[[Any], Any],
    value_fn: Callable[[Any], Any],
    reduce_fn: Callable[[Any, Any], Any],
) -> List[Tuple[Any, Any]]:
    """
    Estilo split-apply-combine:
    - saca clave y valor
    - acumula por clave
    - reduce con reduce_fn
    """
    groups: Dict[Any, List[Any]] = defaultdict(list)
    for item in data:
        k = key_fn(item)
        v = value_fn(item)
        groups[k].append(v)

    result: List[Tuple[Any, Any]] = []
    for k, values in groups.items():
        acc = None
        for v in values:
            acc = v if acc is None else reduce_fn(acc, v)
        result.append((k, acc))
    return result
