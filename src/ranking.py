
"""Funciones puras para ordenar resultados."""

def top_k(conteos, k: int) -> list:
    """Regresa los k pares (clave, valor) con el valor más alto, de mayor a menor.
    No modifica `conteos`: `sorted` crea una lista nueva."""
    return sorted(conteos, key=lambda par: par[1], reverse=True)[:k]
