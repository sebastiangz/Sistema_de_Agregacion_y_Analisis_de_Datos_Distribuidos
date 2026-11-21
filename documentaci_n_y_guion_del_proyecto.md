# Sistema de Agregación y Análisis de Datos Distribuidos (Proyecto 7)

**Equipo 7 – Programación Funcional (2026):**
* **Ailed Camila Gudiño Luna**
* **Camila Gaitán Guzmán**
* **Eduardo Michel Magaña**

**Repositorio:** `https://github.com/sebastiangz/Sistema_de_Agregacion_y_Analisis_de_Datos_Distribuidos`  
**Rama de trabajo:** `equipo7-2026-ailed-camila-eduardo`

---

## 1. ¿Qué es lo que hicimos?

Desarrollamos y reestructuramos un **motor de agregación y análisis de datos distribuidos en Python**, construido bajo los principios de la **Programación Funcional**. El sistema permite tomar grandes volúmenes de información provenientes de múltiples fuentes (como distintas sucursales de una tienda, registros de servidores o archivos de texto), dividirlos en bloques, procesarlos en paralelo utilizando varios núcleos del procesador y consolidar resultados exactos.

### Arquitectura y mejoras técnicas implementadas:
Partimos de una rama base que presentaba fallos de serialización y rutas rotas, y transformamos el proyecto implementando:

1. **Base Matemática con Monoids y Semigroups (`src/monoid.py`):**
   * Implementamos estructuras algebraicas inmutables (`SumMonoid`, `AvgMonoid`, `MinMonoid`, `MaxMonoid`, `product_monoid`, entre otros).
   * Gracias a la **asociatividad** y al **elemento neutro**, garantizamos que dividir los datos entre distintos procesos y combinar los resultados parciales arroje **exactamente el mismo resultado** que procesarlos de forma secuencial, evitando errores matemáticos comunes (como promediar promedios).
2. **Motor Map-Reduce Inmutable y Perezoso (`src/mapreduce.py`):**
   * **Inmutabilidad (`frozen=True`):** Ninguna operación modifica los datos originales; cada transformación devuelve una nueva instancia.
   * **Evaluación Perezosa (*Lazy Evaluation*):** Operaciones como `map`, `flat_map`, `filter`, `key_by` y `map_values` se acumulan en un plan de ejecución y se fusionan en una sola pasada al invocar acciones finales (`collect`, `reduce_by_key`, `aggregate`).
   * **Paralelismo Real:** Ejecuta el flujo clásico **Split → Map + Combiner Local → Shuffle → Reduce** usando `ProcessPoolExecutor` y particionamiento determinista (`CRC32` en `src/partitioners.py`). Si se detectan funciones no serializables (`lambda`), el motor cambia automáticamente a modo secuencial sin romper la ejecución.
3. **Corrección de Errores y Batería de Pruebas (`tests/`):**
   * Corregimos los errores de la base original (`NameError` en ejemplos, rutas incorrectas a `data/samples/` y fallos de `pickle` con lambdas en multiprocesamiento).
   * Pasamos de **7 a 55 pruebas unitarias aprobadas (`pytest`)** y logramos ejecutar un benchmark de **1 millón de líneas en ~0.16 segundos** usando 4 workers.

---

## 2. ¿Dónde se puede aplicar?

Por su diseño ligero, matemático y escalable sin necesidad de infraestructura compleja de servidores, este sistema tiene aplicaciones directas en:

1. **Inteligencia de Negocios para PyMEs y Comercios Multisucursal (Enfoque de Emprendimiento):**
   * Permite a negocios con varias sucursales (como tiendas de retail, cafeterías o distribuidoras) consolidar sus bases de ventas diarias (`CSV`), cruzarlas con catálogos de productos y generar dashboards con ingresos totales, ticket promedio, mediana y márgenes de ganancia por categoría en fracciones de segundo.
2. **Monitoreo de Servidores y Ciberseguridad (Análisis de Logs):**
   * Procesamiento rápido de miles de registros de actividad (`log_analysis.py`) para detectar y cuantificar niveles de alerta (`INFO`, `WARNING`, `ERROR`) en tiempo real y prevenir caídas de sistemas.
3. **Procesamiento de Lenguaje Natural (NLP) y Minería de Texto:**
   * Extracción y conteo masivo de frecuencias de palabras (`word_count.py`) en documentos extensos, reseñas de clientes o redes sociales para análisis de tendencias.
4. **Educación e Investigación en Computación Paralela:**
   * Sirve como herramienta didáctica y reproducible para demostrar cómo la teoría de categorías (Monoids) y la programación funcional resuelven problemas reales de concurrencia y Big Data.

---

## 3. Guion para Video (Explicación y Pitch de Venta)

**Duración estimada:** 1 minuto 30 segundos  
**Objetivo:** Explicar de forma clara el funcionamiento técnico del sistema y vender su utilidad práctica para negocios y análisis de datos.

### [0:00 - 0:20] Introducción y el Problema (Ailed Camila)
* **Visual:** Aparecen los tres integrantes en pantalla o se muestra la portada del proyecto junto con el diagrama del flujo *Datos → Workers → Shuffle → Resultado*.
* **Audio (Ailed):** "¿Alguna vez has intentado unir los reportes de ventas de varias sucursales o analizar millones de registros, y tu computadora se vuelve lenta o los cálculos salen mal? Cuando divides datos para procesarlos rápido, un error tan simple como promediar dos promedios arruina toda tu contabilidad. Hola, somos Ailed, Camila y Eduardo, del Equipo 7, y hoy te presentamos nuestro **Sistema de Agregación y Análisis de Datos Distribuidos**."

### [0:20 - 0:50] La Solución Técnica y Demostración (Camila)
* **Visual:** Grabación de pantalla mostrando la terminal corriendo `pytest` (los 55 tests pasando en verde), el diagrama de las tiendas *Centro* y *Norte*, y el benchmark de 1 millón de líneas.
* **Audio (Camila):** "Construimos en Python un motor Map-Reduce basado en programación funcional. Sus datos son 100% inmutables y su ejecución es perezosa, lo que significa que optimiza cada paso antes de procesarlo. Al usar estructuras matemáticas llamadas **Monoids**, nuestro sistema reparte el trabajo entre varios núcleos de tu procesador y garantiza que el resultado en paralelo sea matemáticamente idéntico al secuencial. De hecho, en nuestras pruebas pasamos de tener errores en la base original a aprobar 55 tests automáticos y procesar **un millón de líneas en solo 0.16 segundos**."

### [0:50 - 1:15] Aplicaciones y Valor de Negocio (Eduardo)
* **Visual:** Se muestra el ejemplo práctico de las tiendas (Bebidas, Comida, Postres, Snacks) y la captura del *Dashboard de Ventas* con métricas de sucursales y márgenes.
* **Audio (Eduardo):** "¿Y dónde puedes aplicar esto? Piensa en una PyME con varias sucursales: nuestro sistema toma las bases de ventas de cada tienda, las cruza con su catálogo y te entrega al instante el ingreso total, el ticket promedio y el margen por categoría en un dashboard claro. Pero no se limita a ventas: también analiza logs de servidores para detectar errores críticos o procesa grandes volúmenes de texto, todo sin pagar licencias costosas en la nube."

### [1:15 - 1:30] Cierre y Llamado a la Acción (Ailed / Todos)
* **Visual:** Pantalla final con el enlace al repositorio de GitHub, el nombre de la rama (`equipo7-2026-ailed-camila-eduardo`) y los nombres del equipo.
* **Audio (Ailed):** "Convierte datos dispersos en decisiones exactas y rápidas. Todo nuestro código está documentado, probado y listo para usarse en nuestro repositorio de GitHub. ¡Entra al enlace en la descripción, clona nuestra rama y pruébalo tú mismo! Muchas gracias."