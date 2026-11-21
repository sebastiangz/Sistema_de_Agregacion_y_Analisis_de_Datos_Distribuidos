# Sistema de Agregación y Análisis de Datos Distribuidos

**Equipo 7 · 2026** · Programación Funcional · Universidad de Colima

| Integrante | Matrícula |
|---|---|
| Ailed Camila Gudiño Luna | 20224860 |
| Camila Gaitán Guzmán | 20221261 |
| Eduardo Michel Magaña | 20221503 |

Sistema en Python que **junta y resume datos de varias fuentes** con el patrón
**Map-Reduce**, en estilo funcional: los datos no se modifican, cada paso es
una función pura y el trabajo se puede repartir entre varios procesos.

## Alcances

- **Map-Reduce funcional:** `map`, `flat_map`, `filter`, `reduce_by_key`,
  `group_by_key` y `aggregate`, inmutable, con evaluación perezosa y
  ejecución en paralelo con varios procesos (workers).
- **Monoids y Semigroups** para combinar resultados en paralelo, con pruebas de
  sus leyes (asociatividad y elemento neutro).
- **Agregadores composables:** suma, conteo, promedio, mínimo, máximo y
  `monoid_aggregator`, que se combinan con `compose_aggregators`.
- **Composición de funciones** con `compose` y `pipe`.
- **Manejo funcional de errores** con Either (`Ok` / `Err`): una fila inválida
  se reporta sin detener el programa.
- **Varias fuentes de datos:** el dashboard junta las ventas de dos sucursales.
- **Caso de uso:** dashboard de ventas en consola, más análisis de logs y
  conteo de palabras.
- **87 pruebas automatizadas** y benchmark de escalabilidad documentado.

## Cómo se cumple cada requisito

| Requisito | Dónde |
|---|---|
| Implementación funcional de Map-Reduce | `src/mapreduce.py` |
| Funciones de agregación composables | `src/aggregators.py` (`compose_aggregators`, `min_aggregator`, `max_aggregator`, `monoid_aggregator`) |
| Paralelismo con inmutabilidad | `MapReduce` es `@dataclass(frozen=True)` y reparte bloques con `ProcessPoolExecutor` |
| Sistema de tipos para garantizar corrección | `reduce_by_key` solo acepta un `Semigroup`/`Monoid`; con otra cosa lanza `TypeError`. Anotaciones genéricas `Monoid[A]` y `MapReduce[T]` |
| Monoids y Semigroups | `src/monoid.py` y la guía [`docs/monoids.md`](docs/monoids.md) |
| Manejo de errores (Maybe/Either) | `src/resultado.py` (`Ok`, `Err`, `intentar`, `separar`) |
| Composición de funciones | `src/funcional.py` (`compose`, `pipe`) |

## Instalación

Requiere Python 3.10 o superior.

```bash
git clone https://github.com/sebastiangz/Sistema_de_Agregacion_y_Analisis_de_Datos_Distribuidos.git
cd Sistema_de_Agregacion_y_Analisis_de_Datos_Distribuidos
python -m pip install pytest
```

El núcleo (`src/mapreduce.py`, `src/monoid.py`, `src/aggregators.py`) solo usa
la biblioteca estándar. `requirements.txt` incluye además las dependencias de
los módulos originales del repositorio (`distributed.py` usa Dask y
`visualization.py` usa Matplotlib).

## Uso

```bash
python -m pytest -q                       # 87 pruebas
python examples/dashboard.py              # dashboard de ventas (2 sucursales)
python examples/sales_analytics.py        # ventas por categoría
python examples/log_analysis.py           # registros por nivel, con porcentaje
python examples/word_count.py             # conteo de palabras
python -m benchmarks.performance_tests    # tiempos con 1, 2 y 4 workers
```

Salida del dashboard:

```
DASHBOARD DE VENTAS · 21 ventas · total $1018.80
--------------------------------------------------------------------------------------------------
Comida    #################### $ 535.70  52.6%  ventas: 6  prom: $89.28  min: $60.20  max: $110.00
Postres   ########             $ 204.50  20.1%  ventas: 4  prom: $51.12  min: $40.00  max: $61.00
Bebidas   ######               $ 162.10  15.9%  ventas: 6  prom: $27.02  min: $18.00  max: $40.10
Snacks    ####                 $ 116.50  11.4%  ventas: 5  prom: $23.30  min: $15.00  max: $32.00
--------------------------------------------------------------------------------------------------
Por fuente: centro $718.80, norte $300.00
Fila ignorada (norte, fila N6: ValueError: could not convert string to float: 'N/A')
Top 3 categorías: Comida ($535.70), Postres ($204.50), Bebidas ($162.10)
```

La fila `N6` de la sucursal norte tiene un monto inválido a propósito: se
reporta como `Err` y el resto de las ventas se procesa normalmente.

### Ejemplos de código

**Map-Reduce con un Monoid:**

```python
from src.mapreduce import MapReduce
from src.monoid import SumMonoid

def tokenizar(linea): return linea.lower().split()
def par_uno(palabra): return (palabra, 1)

conteo = (
    MapReduce.of(lineas, workers=4)
    .flat_map(tokenizar)          # perezoso: aún no se ejecuta
    .map(par_uno)                 # perezoso
    .reduce_by_key(SumMonoid)     # aquí se ejecuta todo, en paralelo
    .sort_by_value(descending=True)
    .collect()
)
```

**Agregadores composables:**

```python
from src.aggregators import compose_aggregators, count_aggregator, max_aggregator, monoid_aggregator
from src.monoid import SumMonoid

resumen = compose_aggregators(
    total=monoid_aggregator(SumMonoid, "amount"),
    ventas=count_aggregator(),
    maximo=max_aggregator("amount"),
)
resumen(ventas)   # {'total': ..., 'ventas': ..., 'maximo': ...}
```

**Manejo de errores con Either y composición:**

```python
from src.funcional import compose, pipe
from src.resultado import intentar, separar

resultados = map(lambda texto: intentar(float, texto), ["12.5", "N/A", "3"])
valores, errores = separar(resultados)      # (12.5, 3.0), ("ValueError: ...",)

pipe(valores, sum, round)                   # round(sum(valores)) -> 16
compose(round, sum)(valores)                # lo mismo, de derecha a izquierda
```

## Estructura

```
src/
  mapreduce.py      Motor Map-Reduce inmutable, perezoso y paralelo
  monoid.py         Semigroup, Monoid e instancias (Sum, Count, Min, Max, Avg...)
  aggregators.py    Agregadores composables
  partitioners.py   Reparto de datos: bloques, hash (shuffle) y rangos
  funcional.py      compose, pipe e identidad
  resultado.py      Either: Ok, Err, intentar y separar
  metricas.py       porcentaje
  ranking.py        top_k
  combiners.py, distributed.py, visualization.py   Módulos originales del repositorio
examples/           dashboard, sales_analytics, log_analysis y word_count
tests/              87 pruebas (pytest)
benchmarks/         performance_tests.py
data/samples/       CSV de ventas (2 sucursales), logs y texto de ejemplo
docs/               Arquitectura, API, Monoids y resultados del benchmark
```

## Documentación

- [`docs/architecture.md`](docs/architecture.md): módulos, flujo de `reduce_by_key` y decisiones de diseño.
- [`docs/api_reference.md`](docs/api_reference.md): referencia de todas las funciones.
- [`docs/monoids.md`](docs/monoids.md): qué es un Monoid y por qué permite paralelizar.
- [`docs/benchmarks.md`](docs/benchmarks.md): resultados y análisis del benchmark.
- Las funciones principales documentan sus contratos con `Pre:` y `Post:` en el docstring.

## Resultados del benchmark

Conteo de palabras en 1 millón de líneas (Linux, 4 CPUs, mediana de 3 ejecuciones):

| Workers | Tiempo | Speedup |
|---:|---:|---:|
| 1 | 0.70 s | 1.00× |
| 2 | 0.42 s | 1.67× |
| 4 | 0.23 s | 3.04× |

El resultado es idéntico con cualquier número de workers, porque la suma es un
Monoid. No escala de forma lineal por el costo de crear procesos y enviarles
los datos. Análisis completo en [`docs/benchmarks.md`](docs/benchmarks.md).

## Avance por semana

| Semana | Entregable | Qué se hizo |
|---|---|---|
| 1 | Framework básico de Map-Reduce funcional | Motor `MapReduce` inmutable y paralelo, Monoids y Semigroups, particionadores; se corrigieron los errores de la base (PR #1) |
| 2 | Operadores de agregación composables | `min_aggregator`, `max_aggregator`, `monoid_aggregator`, `porcentaje` y `top_k`, uno por integrante con su prueba (PR #8, #9 y #11) |
| 3 | Sistema de procesamiento paralelo | Workers con combiner local y shuffle; lectura de varias fuentes con manejo de errores Either |
| 4 | Casos de uso y benchmarks | Dashboard de ventas en consola, análisis de logs, conteo de palabras y benchmark documentado |

## Referencias

1. Dean, J., & Ghemawat, S. (2008). MapReduce: Simplified data processing on
   large clusters. *Communications of the ACM, 51*(1), 107–113.
   https://doi.org/10.1145/1327452.1327492
2. Dean, J., & Ghemawat, S. (2010). MapReduce: A flexible data processing tool.
   *Communications of the ACM, 53*(1), 72–77.
   https://doi.org/10.1145/1629175.1629198
3. Yu, Y., Gunda, P. K., & Isard, M. (2009). Distributed aggregation for
   data-parallel computing: Interfaces and implementations. *Proceedings of
   the ACM SIGOPS 22nd Symposium on Operating Systems Principles (SOSP '09)*,
   247–260. https://doi.org/10.1145/1629575.1629600
4. Zaharia, M., Xin, R. S., Wendell, P., Das, T., Armbrust, M., Dave, A.,
   Meng, X., Rosen, J., Venkataraman, S., Franklin, M. J., Ghodsi, A.,
   Gonzalez, J., Shenker, S., & Stoica, I. (2016). Apache Spark: A unified
   engine for big data processing. *Communications of the ACM, 59*(11),
   56–65. https://doi.org/10.1145/2934664

## Uso de herramientas de inteligencia artificial

En este proyecto se utilizó Claude (Anthropic), un asistente de inteligencia
artificial, como apoyo para generar código base, pruebas, documentación y la
planeación de los commits. El equipo revisó, ejecutó y validó el código con
pruebas automatizadas y es responsable de su contenido.

**Cita (APA 7):**
Anthropic. (2026). *Claude* [Modelo de lenguaje de gran tamaño]. https://claude.ai

---

El README original del repositorio se conserva en
[`docs/README_original.md`](docs/README_original.md).
