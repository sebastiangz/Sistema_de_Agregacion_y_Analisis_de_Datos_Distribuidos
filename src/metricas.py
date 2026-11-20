"""Funciones puras de métricas simples."""


def porcentaje(parte: float, total: float) -> float:
    """Regresa qué porcentaje es `parte` de `total`, de 0 a 100.

    Si el total es 0 regresa 0.0 en lugar de dividir entre cero.
    """
    if total == 0:
        return 0.0
    return parte / total * 100