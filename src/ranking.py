
"""Funciones puras para ordenar resultados."""


def top_k(conteos, k: int) -> list:
    """Regresa los k pares (clave, valor) con el valor más alto, de mayor a menor.

    Pre: `conteos` es una colección de pares (clave, valor) con valores
         comparables; `k` >= 0.
    Post: lista nueva con a lo más k pares. `conteos` no se modifica
          (`sorted` crea una lista nueva). Con empates conserva el orden original.
    """
    return sorted(conteos, key=lambda par: par[1], reverse=True)[:k]
