from src.mapreduce import MapReduce


def test_large_dataset():
    data = [f"line {i}" for i in range(10000)]
    mr = MapReduce().load(data)
    
    result = (
        mr
        .map(lambda line: (line, 1))
        .reduce_by_key(lambda a, b: a + b)
        .collect()
    )
    
    assert len(result) == 10000
    # Opcional: validar que todos tengan 1
    for _, count in result:
        assert count == 1

