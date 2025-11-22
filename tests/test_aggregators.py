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


