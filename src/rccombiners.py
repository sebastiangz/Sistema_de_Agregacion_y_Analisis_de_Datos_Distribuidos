from typing import Callable, Iterable, Tuple, Any, Dict
from collections import defaultdict

def local_combiner(data: Iterable[Tuple[Any, Any]], combine_fn: Callable[[Any, Any], Any]) -> Dict[Any, Any]:
    """
    Aplica un combiner local: agrupa por clave y combina valores localmente.
    Retorna dict {key: combined_value}
    """
    acc = {}
    for k, v in data:
        if k in acc:
            acc[k] = combine_fn(acc[k], v)
        else:
            acc[k] = v
    return acc

def merge_combined(dicts: Iterable[Dict[Any, Any]], combine_fn: Callable[[Any, Any], Any]) -> Dict[Any, Any]:
    out = {}
    for d in dicts:
        for k, v in d.items():
            if k in out:
                out[k] = combine_fn(out[k], v)
            else:
                out[k] = v
    return out

