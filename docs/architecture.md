# Arquitectura

El proyecto procesa datos con el patrón **Map-Reduce** en estilo funcional: los datos no se modifican, cada paso es una función pura y el trabajo se reparte entre varios procesos.

## Módulos

- **`monoid.py`**: Semigroup y Monoid. Operaciones asociativas con elemento neutro (suma, conteo, mínimo, máximo, promedio, etc.). Son la base para combinar resultados en paralelo.
- **`mapreduce.py`**: El motor. `MapReduce` es inmutable: cada operación (`map`, `flat_map`, `filter`, `reduce_by_key`, `sort_by_value`, etc.) devuelve un objeto nuevo. Las transformaciones se ejecutan hasta pedir el resultado (evaluación perezosa).
- **`partitioners.py`**: Reparte los datos en bloques (`chunk`), por clave (`hash_partition`) o por rangos (`range_partition`).
- **`aggregators.py`**: Agregadores composables como `sum`, `count`, `avg` `compose`, `min`, `max` y `monoid_aggregator` (convierte cualquier Monoid en agregador).
- **`funcional.py`**: Composición de funciones con `compose` (derecha a izquierda) y `pipe` (izquierda a derecha).
- **`resultado.py`**: Manejo funcional de errores con Either: `Ok` y `Err`. Un dato inválido se convierte en `Err` en lugar de lanzar una excepción.
- **`metricas.py`** y **`ranking.py`**: funciones puras `porcentaje` y `top_k`.
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

## Lectura de varias fuentes con manejo de errores

El dashboard (`examples/dashboard.py`) separa los efectos de lo puro:

1. `leer_fuente` es el único lugar que abre archivos. Convierte cada fila en
   `Ok(venta)` o `Err(mensaje)`.
2. `leer_fuentes` junta las filas de todas las sucursales y las separa en
   `(ventas válidas, errores)` con `separar`.
3. `dashboard` es una función pura: recibe las ventas y los errores y
   regresa el texto. Una fila inválida aparece como "Fila ignorada" y no
   detiene el procesamiento del resto.
