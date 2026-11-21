# Resultados del benchmark

Comando:

```bash
python -m benchmarks.performance_tests
```

Cuenta las palabras de **1 millón de líneas** (`"palabra uno dos uno"`) con
`MapReduce` y `reduce_by_key(SumMonoid)`, usando 1, 2 y 4 procesos (workers).
En todos los casos el resultado es el mismo:
`uno = 2,000,000`, `dos = 1,000,000`, `palabra = 1,000,000`.

## Resultados

Máquina: Linux, 4 CPUs, Python 3.13. Se tomó la mediana de 3 ejecuciones.

| Workers | Tiempo | Speedup (vs. 1 worker) | Eficiencia |
|---:|---:|---:|---:|
| 1 | 0.70 s | 1.00× | 100 % |
| 2 | 0.42 s | 1.67× | 83 % |
| 4 | 0.23 s | 3.04× | 76 % |

Las 3 ejecuciones con 4 workers tardaron 0.21 s, 0.26 s y 0.23 s. Los tiempos
cambian según la computadora y lo que esté corriendo al mismo tiempo.

## Análisis

1. **El resultado es idéntico con cualquier número de workers.** Es la
   garantía de usar un Monoid: la suma es asociativa, así que sumar por
   partes y después juntar los parciales da lo mismo que sumar todo en orden.
2. **Escala bien, pero no de forma lineal (3.04× con 4 workers, no 4×).**
   Repartir el trabajo tiene un costo que no se paraleliza: crear los
   procesos y enviarles los datos (Python los serializa con pickle). Por la
   ley de Amdahl, esa parte secuencial limita la mejora.
3. **El combiner local ayuda mucho.** Cada worker suma sus propios pares
   antes del shuffle, así que regresa solo 3 claves (`uno`, `dos`,
   `palabra`) en lugar de 4 millones de pares `(palabra, 1)`. Casi todo el
   costo de comunicación queda en enviar las líneas de entrada.
4. **Con pocos datos conviene no paralelizar.** Por eso `MapReduce` tiene
   `parallel_threshold` (5,000 elementos por defecto): con menos datos
   trabaja en un solo proceso, porque crear procesos tardaría más que el
   cálculo.
