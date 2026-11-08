# API Reference

## MapReduce (`src/mapreduce.py`)

Objeto inmutable: cada operación devuelve un MapReduce nuevo.

- `MapReduce.of(data, workers=None, parallel_threshold=5000)`: crea una colección (`workers=None` usa todos los CPUs)
- `MapReduce().load(data)`: API original; devuelve un MapReduce nuevo con esos datos
- `map(func)`, `flat_map(func)`, `filter(func)`, `map_values(func)`, `key_by(func)`: perezosas
- `reduce_by_key(semigroup)`: requiere un `Semigroup` o `Monoid` (por ejemplo `SumMonoid`)
- `aggregate(monoid)`: reduce toda la colección con un `Monoid`
- `group_by_key()`, `sort_by_value(descending=False)`, `sort_by_key(descending=False)`, `take(n)`, `cache()`
- `collect()`, `count()`, `to_dict()`

## Monoids (`src/monoid.py`)

- `Semigroup(combine)`, `Monoid(combine, empty)`
- Instancias: `SumMonoid`, `ProductMonoid`, `CountMonoid`, `MinMonoid`, `MaxMonoid`, `AvgMonoid`, `AllMonoid`, `AnyMonoid`, `FirstSemigroup`, `LastSemigroup`
- `product_monoid(**monoids)`: combina varios monoids en uno
- `check_associativity(sg, a, b, c)`, `check_identity(m, a)`

## Partitioners (`src/partitioners.py`)

- `chunk(data, n)`, `hash_partition(pairs, n)`, `range_partition(data, key_fn, boundaries)`
- Originales: `partition(data, n)`, `partition_by_key(data, key_fn, n)`, `partition_by_range(data, key_fn, ranges)`

## Aggregators (`src/aggregators.py`)

- `sum_aggregator(field=None)`
- `count_aggregator()`
- `avg_aggregator(field=None)`
- `compose_aggregators(**aggs)`
