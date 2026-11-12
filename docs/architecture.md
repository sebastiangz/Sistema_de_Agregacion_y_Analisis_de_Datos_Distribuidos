# Arquitectura

El proyecto procesa datos con el patrón **Map-Reduce** en estilo funcional: los datos no se modifican, cada paso es una función pura y el trabajo se reparte entre varios procesos.

## Módulos

- **`monoid.py`**: Semigroup y Monoid. Operaciones asociativas con elemento neutro (suma, conteo, mínimo, máximo, promedio, etc.). Son la base para combinar resultados en paralelo.
- **`mapreduce.py`**: El motor. `MapReduce` es inmutable: cada operación (`map`, `flat_map`, `filter`, `reduce_by_key`, `sort_by_value`, etc.) devuelve un objeto nuevo. Las transformaciones se ejecutan hasta pedir el resultado (evaluación perezosa).
- **`partitioners.py`**: Reparte los datos en bloques (`chunk`), por clave (`hash_partition`) o por rangos (`range_partition`).
- **`aggregators.py`**: Agregadores composables como `sum`, `count`, `avg` y `compose`.
- **`combiners.py`**: Combinación local de pares antes de la reducción global.
- **`distributed.py`**: Ejemplos de pipeline paralelo usando Dask Bag.
- **`visualization.py`**: Funciones para mostrar resultados en texto y gráfica.

## Flujo de `reduce_by_key`

1. **Split**: Los datos se dividen en bloques, uno por proceso (*worker*).
2. **Map + Combiner**: Cada *worker* transforma su bloque y suma localmente.
3. **Shuffle**: Los pares con la misma clave se juntan en la misma partición.
4. **Reduce**: Cada partición se reduce con un `Monoid`. Como la operación del Monoid es asociativa, el resultado en paralelo es el mismo que si todo se calculara en orden.

## Decisiones de diseño

- `reduce_by_key` solo acepta un `Semigroup` o `Monoid` (por ejemplo, `SumMonoid`); si recibe una función cualquiera, lanza `TypeError`.
- Las funciones que corren en paralelo se definen con `def` y no como `lambda`, porque Python no puede serializar y enviar lambdas a otros procesos.
- Los ejemplos y tests demuestran el diseño modular y extensible.
