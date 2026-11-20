from src.aggregators import sum_aggregator, count_aggregator, avg_aggregator


def test_sum_aggregator():
    agg = sum_aggregator()
    data = [1, 2, 3, 4]
    assert agg.aggregate(data) == 10


def test_sum_aggregator_field():
    agg = sum_aggregator("value")
    data = [{"value": 1}, {"value": 2}, {"value": 3}]
    assert agg.aggregate(data) == 6


def test_count_aggregator():
    agg = count_aggregator()
    data = [None, None, None]
    assert agg.aggregate(data) == 3


def test_avg_aggregator():
    agg = avg_aggregator()
    data = [2, 4, 6, 8]
    assert agg.aggregate(data) == 5.0


def test_avg_aggregator_field():
    agg = avg_aggregator("value")
    data = [{"value": 2}, {"value": 4}, {"value": 6}]
    assert agg.aggregate(data) == 4.0


def test_min_aggregator():
    from src.aggregators import min_aggregator

    assert min_aggregator().aggregate([5, 2, 8]) == 2
    assert min_aggregator("value").aggregate([{"value": 4}, {"value": 1}]) == 1
    assert min_aggregator().aggregate([]) is None

def test_max_aggregator():
    from src.aggregators import max_aggregator
    assert max_aggregator().aggregate([5, 2, 8]) == 8
    assert max_aggregator("value").aggregate([{"value": 4}, {"value": 1}]) == 4
    assert max_aggregator().aggregate([]) is None
 

def test_monoid_aggregator_suma_igual_que_sum():
    from src.aggregators import monoid_aggregator
    from src.monoid import SumMonoid
    ventas = [{"amount": 10.0}, {"amount": 5.5}, {"amount": 4.5}]
    assert monoid_aggregator(SumMonoid, "amount").aggregate(ventas) == 20.0

def test_monoid_aggregator_partes_combinadas_igual_al_todo():
    from src.aggregators import monoid_aggregator
    from src.monoid import MaxMonoid
    agg = monoid_aggregator(MaxMonoid)
    datos = [3, 9, 1, 7, 4]
    parte1, parte2 = agg.aggregate(datos[:2]), agg.aggregate(datos[2:])
    assert agg.combiner(parte1, parte2) == agg.aggregate(datos) == 9



