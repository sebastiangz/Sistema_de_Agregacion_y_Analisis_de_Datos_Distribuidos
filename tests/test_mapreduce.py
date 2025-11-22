import pytest
from src.mapreduce import MapReduce


def test_load_and_collect():
    data = ["a", "b", "c"]
    mr = MapReduce().load(data)
    collected = mr.collect()
    assert collected == data


def test_map_flatmap_reduce():
    data = ["a b a", "b c"]
    mr = MapReduce().load(data)

    # Cuenta palabras
    result = (
        mr
        .map(lambda line: line.split())
        .flat_map(lambda ws: [(w, 1) for w in ws])
        .reduce_by_key(lambda a, b: a + b)
        .sort_by_value()
        .collect()
    )

    counts = dict(result)
    assert counts.get("a") == 2
    assert counts.get("b") == 2
    assert counts.get("c") == 1


def test_filter():
    data = [1, 2, 3, 4, 5]
    mr = MapReduce().load(data)

    filtered = mr.filter(lambda x: x % 2 == 0).collect()
    assert filtered == [2, 4]


