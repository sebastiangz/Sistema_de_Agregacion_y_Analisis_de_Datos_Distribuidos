"""Funciones puras de métricas simples."""


def porcentaje(parte: float, total: float) -> float:
    """Regresa qué porcentaje es `parte` de `total`.

    Pre: `parte` y `total` son números.
    Post: regresa parte / total * 100. Si `total` es 0 regresa 0.0 en lugar
          de dividir entre cero. No modifica nada.
    """
    if total == 0:
        return 0.0
    return parte / total * 100