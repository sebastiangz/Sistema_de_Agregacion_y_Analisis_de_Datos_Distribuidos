# Arquitectura

El proyecto está dividido en módulos funcionales independientes:

- mapreduce.py: Pipeline tipo MapReduce (map, flat_map, filter, reduce_by_key, sort).
- aggregators.py: Agregadores composables como sum, count, avg, compose.
- partitioners.py: Particionamiento de datos en chunks, por clave y por rangos.
- combiners.py: Combinación local de pares antes de reducción global.
- distributed.py: Ejemplos de pipeline paralelo usando Dask Bag.
- visualization.py: Funciones para mostrar resultados en texto y gráfica.

Los ejemplos y tests demuestran el diseño modular y extensible inspirado en MapReduce.

