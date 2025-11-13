# 🌐 Proyecto 7: Sistema de Agregación y Análisis de Datos Distribuidos
📋 Descripción del Proyecto
Sistema funcional para procesamiento distribuido de grandes volúmenes de datos utilizando el paradigma MapReduce, operaciones de agregación composables y análisis estadístico paralelo.

Universidad de Colima - Ingeniería en Computación Inteligente
Materia: Programación Funcional
Profesor: Gonzalez Zepeda Sebastian
Semestre: Agosto 2025 - Enero 2026

🎯 Objetivos
Implementar MapReduce funcional desde cero
Desarrollar operaciones de agregación composables
Aplicar paralelismo funcional con funciones puras
Crear combinadores para análisis distribuido
Utilizar particionamiento funcional de datos
Practicar lazy evaluation en grandes datasets

🛠️ Tecnologías Utilizadas
Lenguaje: Python 3.11+
Paradigma: Programación Funcional + Distribuida
Librerías:
multiprocessing - Paralelismo
dask - Computación paralela
toolz - Utilidades funcionales
pandas - DataFrames
pyarrow - Serialización eficiente

📦 Instalación
```bash
# Clonar el repositorio
git clone https://github.com/tu-usuario/distributed-data-analysis.git
cd distributed-data-analysis

# Crear entorno virtual
python -m venv venv
source venv/bin/activate

# Instalar dependencias
pip install -r requirements.txt
```

**requirements.txt**
```
dask>=2023.12.0
distributed>=2023.12.0
toolz>=0.12.0
pandas>=2.0.0
pyarrow>=14.0.0
numpy>=1.24.0
multiprocessing-logging>=0.3.0
```

🚀 Uso del Sistema
```python
from src.mapreduce import MapReduce
from src.aggregators import sum_aggregator, count_aggregator
from toolz import compose

# Crear instancia de MapReduce
mr = MapReduce(workers=4)

# Pipeline funcional distribuido
result = (mr
    .map(lambda line: line.strip().split())
    .flat_map(lambda words: [(w, 1) for w in words])
    .reduce_by_key(lambda a, b: a + b)
    .sort_by_value(descending=True)
    .collect())

# Agregaciones composables
analysis = (dataset
    .partition_by('category')
    .aggregate(
        total=sum_aggregator('amount'),
        count=count_aggregator(),
        avg=compose(sum_aggregator('amount'), count_aggregator())
    )
    .compute())
```

📂 Estructura del Proyecto
```
distributed-data-analysis/
├── src/
│   ├── __init__.py
│   ├── mapreduce.py        # Implementación MapReduce funcional
│   ├── aggregators.py      # Agregadores composables
│   ├── partitioners.py     # Estrategias de particionamiento
│   ├── combiners.py        # Combinadores funcionales
│   ├── distributed.py      # Coordinación distribuida
│   └── visualization.py    # Visualización de resultados
├── tests/
│   ├── test_mapreduce.py
│   ├── test_aggregators.py
│   └── test_performance.py
├── examples/
│   ├── word_count.py       # Conteo de palabras
│   ├── log_analysis.py     # Análisis de logs
│   └── sales_analytics.py  # Análisis de ventas
├── data/
│   ├── sample/             # Datasets de ejemplo
│   └── output/             # Resultados
├── benchmarks/
│   └── performance_tests.py
├── docs/
│   ├── architecture.md
│   └── api_reference.md
├── requirements.txt
├── README.md
└── .gitignore
```

🔑 Características Principales

1. MapReduce Funcional
```python
from typing import Callable, Iterable, TypeVar, List, Tuple
from functools import reduce
from itertools import groupby
from multiprocessing import Pool

K = TypeVar('K')
V = TypeVar('V')

class MapReduce:
    """Implementación funcional de MapReduce"""
    
    def __init__(self, workers: int = 4):
        self.workers = workers
        self.data = []
    
    def map(self, mapper: Callable) -> 'MapReduce':
        """Map: aplicar función a cada elemento"""
        with Pool(self.workers) as pool:
            self.data = pool.map(mapper, self.data)
        return self
    
    def flat_map(self, mapper: Callable) -> 'MapReduce':
        """FlatMap: map que aplana resultados"""
        with Pool(self.workers) as pool:
            mapped = pool.map(mapper, self.data)
            self.data = [item for sublist in mapped for item in sublist]
        return self
    
    def reduce_by_key(self, reducer: Callable[[V, V], V]) -> 'MapReduce':
        """Reduce: agregar valores por clave"""
        # Ordenar por clave para groupby
        sorted_data = sorted(self.data, key=lambda x: x[0])
        
        # Agrupar y reducir
        result = []
        for key, group in groupby(sorted_data, key=lambda x: x[0]):
            values = [item[1] for item in group]
            reduced_value = reduce(reducer, values)
            result.append((key, reduced_value))
        
        self.data = result
        return self
    
    def filter(self, predicate: Callable) -> 'MapReduce':
        """Filter: filtrar elementos"""
        self.data = list(filter(predicate, self.data)
        return self
    
    def collect(self) -> List:
        """Collect: materializar resultados"""
        return self.data
```

2. Agregadores Composables
```python
from dataclasses import dataclass
from typing import Callable, Any, List
from functools import reduce

@dataclass(frozen=True)
class Aggregator:
    """Agregador funcional composable"""
    initializer: Callable[[], Any]
    accumulator: Callable[[Any, Any], Any]
    combiner: Callable[[Any, Any], Any]
    finalizer: Callable[[Any], Any] = lambda x: x
    
    def aggregate(self, data: Iterable) -> Any:
        """Aplicar agregación"""
        acc = self.initializer()
        for item in data:
            acc = self.accumulator(acc, item)
        return self.finalizer(acc)
    
    def map(self, fn: Callable) -> 'Aggregator':
        """Transformar resultado final"""
        return Aggregator(
            self.initializer,
            self.accumulator,
            self.combiner,
            lambda x: fn(self.finalizer(x))
        )

# Agregadores predefinidos
def sum_aggregator(field: str = None):
    """Agregador de suma"""
    return Aggregator(
        initializer=lambda: 0,
        accumulator=lambda acc, x: acc + (x[field] if field else x),
        combiner=lambda a, b: a + b
    )

def count_aggregator():
    """Agregador de conteo"""
    return Aggregator(
        initializer=lambda: 0,
        accumulator=lambda acc, x: acc + 1,
        combiner=lambda a, b: a + b
    )

def avg_aggregator(field: str = None):
    """Agregador de promedio"""
    return Aggregator(
        initializer=lambda: (0, 0),  # (sum, count)
        accumulator=lambda acc, x: (
            acc[0] + (x[field] if field else x),
            acc[1] + 1
        ),
        combiner=lambda a, b: (a[0] + b[0], a[1] + b[1]),
        finalizer=lambda x: x[0] / x[1] if x[1] > 0 else 0
    )

def compose_aggregators(**aggregators):
    """Componer múltiples agregadores"""
    def combined(data):
        return {
            name: agg.aggregate(data)
            for name, agg in aggregators.items()
        }
    return combined
```

3. Particionamiento Funcional
```python
from typing import Callable, List, TypeVar
from itertools import islice

T = TypeVar('T')

def partition(data: Iterable[T], n: int) -> List[List[T]]:
    """Particionar datos en n chunks"""
    data_list = list(data)
    chunk_size = len(data_list) // n
    remainder = len(data_list) % n
    
    chunks = []
    start = 0
    for i in range(n):
        # Distribuir remainder entre chunks
        size = chunk_size + (1 if i < remainder else 0)
        chunks.append(data_list[start:start + size])
        start += size
    
    return chunks

def partition_by_key(data: Iterable[Tuple[K, V]], 
                     key_fn: Callable[[K], int],
                     n: int) -> List[List[Tuple[K, V]]]:
    """Particionar por hash de clave"""
    partitions = [[] for _ in range(n)]
    for item in data:
        key, value = item
        partition_id = key_fn(key) % n
        partitions[partition_id].append(item)
    return partitions

def partition_by_range(data: Iterable[T],
                       key_fn: Callable[[T], Any],
                       ranges: List[Tuple]) -> List[List[T]]:
    """Particionar por rangos de valores"""
    partitions = [[] for _ in range(len(ranges) + 1)]
    for item in data:
        value = key_fn(item)
        partition_id = 0
        for i, (start, end) in enumerate(ranges):
            if start <= value < end:
                partition_id = i
                break
        partitions[partition_id].append(item)
    return partitions
```

4. Pipeline Distribuido Completo
```python
from toolz import compose, pipe

def create_distributed_pipeline(workers=4):
    """Crear pipeline de procesamiento distribuido"""
    
    mr = MapReduce(workers=workers)
    
    # Pipeline composable
    pipeline = compose(
        # 1. Cargar y particionar datos
        lambda data: mr.load(data),
        
        # 2. Mapear: extraer características
        lambda mr: mr.map(extract_features),
        
        # 3. Filter: remover inválidos
        lambda mr: mr.filter(is_valid),
        
        # 4. FlatMap: expandir elementos
        lambda mr: mr.flat_map(expand_items),
        
        # 5. Reduce: agregar por clave
        lambda mr: mr.reduce_by_key(lambda a, b: a + b),
        
        # 6. Sort: ordenar resultados
        lambda mr: mr.sort_by_value(descending=True),
        
        # 7. Collect: materializar
        lambda mr: mr.collect()
    )
    
    return pipeline

# Uso
pipeline = create_distributed_pipeline(workers=8)
results = pipeline(large_dataset)
```

📊 Funcionalidades Implementadas

**MapReduce**
✅ Map, FlatMap, Filter
✅ ReduceByKey con combiners
✅ GroupByKey eficiente
✅ Join distribuido

**Agregación**
✅ Sum, Count, Avg, Min, Max
✅ Percentiles distribuidos
✅ Agregaciones custom composables
✅ Window functions

**Particionamiento**
✅ Hash partitioning
✅ Range partitioning
✅ Custom partitioners
✅ Rebalanceo dinámico

**Optimización**
✅ Lazy evaluation
✅ Combiners locales
✅ Serialización eficiente
✅ Caching inteligente

🧪 Testing
```bash
# Tests unitarios
pytest tests/ -v

# Tests de performance
pytest benchmarks/ -v

# Tests con datasets grandes
pytest tests/ -k "large" --timeout=300
```

📈 Pipeline de Desarrollo

**Semana 1: MapReduce Base (30 Oct - 5 Nov)**
Map, Reduce básico
Particionamiento
Paralelismo con multiprocessing

**Semana 2: Operaciones Avanzadas (6 Nov - 12 Nov)**
Agregadores composables
Join distribuido
Optimizaciones

**Semana 3: Análisis Estadístico (13 Nov - 19 Nov)**
Estadísticas distribuidas
Visualización
Casos de uso reales

**Semana 4: Optimización (20 Nov)**
Benchmarks
Tuning de performance
Documentación

💼 Componente de Emprendimiento

**Aplicación Real:** Plataforma de análisis de datos para empresas

**Propuesta de Valor:**
Procesamiento de TB de datos en minutos
Escalabilidad horizontal
API simple y funcional
Costos reducidos vs soluciones cloud

**Casos de Uso:**
E-commerce: Análisis de comportamiento de usuarios
Finance: Detección de fraude en transacciones
IoT: Procesamiento de datos de sensores
Marketing: Análisis de campañas multicanal

📚 Referencias
Dean, J., & Ghemawat, S. (2004). MapReduce: Simplified Data Processing on Large Clusters
Dask Documentation: https://docs.dask.org/
Toolz Documentation: https://toolz.readthedocs.io/
Apache Spark Programming Guide

🏆 Criterios de Evaluación
MapReduce Funcional (30%): Corrección, paralelismo eficiente
Agregadores Composables (25%): Diseño elegante, reusabilidad
Performance (25%): Escalabilidad, benchmarks
Testing y Documentación (20%): Cobertura, claridad
