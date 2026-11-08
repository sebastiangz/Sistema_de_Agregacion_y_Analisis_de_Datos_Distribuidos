from src.mapreduce import MapReduce
from src.monoid import SumMonoid


def line_to_pair(line):
    return (line, 1)


def test_large_dataset():
    data = [f"line {i}" for i in range(10000)]
    mr = MapReduce.of(data, workers=2)

    result = (
        mr
        .map(line_to_pair)
        .reduce_by_key(SumMonoid)
        .collect()
    )

    assert len(result) == 10000
    # Opcional: validar que todos tengan 1
    for _, count in result:
        assert count == 1
