from typing import Iterable, Any, Callable
import dask.bag as db


def distributed_word_count(lines: Iterable[str]) -> list[tuple[str, int]]:
    """
    Ejemplo MapReduce básico usando Dask Bag.
    """
    bag = db.from_sequence(list(lines), npartitions=4)

    words = (
        bag
        .map(lambda line: line.strip().split())
        .flatten()
    )

    # foldby = groupby + reduce eficiente
    counts = words.foldby(
        key=lambda w: w,
        binop=lambda acc, _: acc + 1,
        initial=0,
        combine=lambda a, b: a + b,
    )

    result = list(counts.compute())
    result.sort(key=lambda x: x[1], reverse=True)
    return result


def run_generic_bag_pipeline(
    data: Iterable[Any],
    map_fn: Callable[[Any], Any],
    filter_fn: Callable[[Any], bool] | None = None,
) -> list[Any]:
    """
    Pipeline genérico en Dask Bag: map -> (filter) -> collect.
    """
    bag = db.from_sequence(list(data), npartitions=4)
    bag = bag.map(map_fn)
    if filter_fn is not None:
        bag = bag.filter(filter_fn)
    return list(bag.compute())

