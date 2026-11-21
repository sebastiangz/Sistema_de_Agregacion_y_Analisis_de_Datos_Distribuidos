"""Utilidades de composición de funciones."""
from functools import reduce
from typing import Any, Callable


def identidad(x: Any) -> Any:
    """Regresa el mismo valor. Es el neutro de la composición."""
    return x


def compose(*funciones: Callable[[Any], Any]) -> Callable[[Any], Any]:
    """Compone funciones de derecha a izquierda: compose(f, g)(x) == f(g(x)).

    Pre: cada función recibe un solo argumento.
    Post: regresa una función nueva; no ejecuta nada hasta que se llama.
          compose() sin funciones es la identidad.
    """
    return reduce(lambda f, g: lambda x: f(g(x)), funciones, identidad)


def pipe(valor: Any, *funciones: Callable[[Any], Any]) -> Any:
    """Pasa un valor por varias funciones de izquierda a derecha.

    pipe(x, f, g) == g(f(x)).
    Pre: cada función recibe un solo argumento.
    Post: regresa el resultado de la última función; `valor` no se modifica.
    """
    return reduce(lambda acumulado, f: f(acumulado), funciones, valor)
