# API Reference

## MapReduce

- load(data)
- map(func)
- flat_map(func)
- filter(func)
- reduce_by_key(func)
- sort_by_value(descending=False)
- sort_by_key(descending=False)
- collect()

## Aggregators

- sum_aggregator(field=None)
- count_aggregator()
- avg_aggregator(field=None)
- compose_aggregators(**aggs)

