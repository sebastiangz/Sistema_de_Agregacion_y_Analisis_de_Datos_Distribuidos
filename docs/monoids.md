# ¿Qué es un Monoid? (guía para el equipo)
Un **Monoid** es una forma de combinar datos que cumple dos reglas:
1. **Asociativa:** no importa cómo agrupes, el resultado es el mismo.
   `(2 + 3) + 4 = 2 + (3 + 4) = 9`
2. **Tiene un neutro:** un valor que no cambia nada al combinarse.
   En la suma es `0`, porque `0 + x = x`.
Si solo cumple la regla 1 se llama **Semigroup**.
.# Por qué nos sirve
Como no importa el orden de los paréntesis, podemos **partir los datos**:
```
Worker 1: 1 + 2 + 3 + 4 = 10
Worker 2: 5 + 6 + 7 + 8 = 26
Juntar:   10 + 26       = 36   (igual que sumar todo en orden)
```
Por eso `reduce_by_key` solo acepta un Monoid: así sabemos que el resultado
en paralelo es correcto.
.# Cuidado: el promedio NO es un Monoid
`prom(prom(2, 4), 9) = 6`, pero `prom(2, 4, 9) = 5`.
La solución es guardar `(suma, cantidad)`, que sí se puede sumar
(`AvgMonoid`), y dividir una sola vez al final.
.# En el código
| Monoid | combine | neutro |
|..-|..-|..-|
| `SumMonoid` | suma | `0` |
| `CountMonoid` | suma de 1s | `0` |
| `MaxMonoid` | el mayor | `-inf` |
| `MinMonoid` | el menor | `inf` |
| `AvgMonoid` | suma de pares `(suma, n)` | `(0, 0)` |
`monoid_aggregator(monoid, campo)` en `src/aggregators.py` convierte
cualquiera de ellos en un agregador:
```python
from src.aggregators import monoid_aggregator
from src.monoid import SumMonoid
total = monoid_aggregator(SumMonoid, "amount").aggregate(ventas)
```
El dashboard (`examples/dashboard.py`) lo usa para calcular el total de ventas.