"""Manejo funcional de errores con el patrón Either: Ok (éxito) o Err (error).

En lugar de lanzar una excepción que detiene el programa, una función
regresa un valor Ok o Err. Así los errores son datos: se pueden filtrar,
contar y reportar sin interrumpir el procesamiento del resto.
"""
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Tuple, Union


@dataclass(frozen=True)
class Ok:
    """Resultado exitoso que contiene un valor."""

    valor: Any
    es_ok = True

    def map(self, fn: Callable[[Any], Any]) -> "Resultado":
        """Aplica `fn` al valor. Post: regresa un Ok nuevo."""
        return Ok(fn(self.valor))

    def bind(self, fn: Callable[[Any], "Resultado"]) -> "Resultado":
        """Encadena una función que también regresa Ok o Err."""
        return fn(self.valor)


@dataclass(frozen=True)
class Err:
    """Resultado fallido que contiene la descripción del error."""

    error: str
    es_ok = False

    def map(self, fn: Callable[[Any], Any]) -> "Resultado":
        """Un error no se transforma: Post: regresa el mismo Err."""
        return self

    def bind(self, fn: Callable[[Any], "Resultado"]) -> "Resultado":
        """Un error corta la cadena: Post: regresa el mismo Err."""
        return self


Resultado = Union[Ok, Err]


def intentar(fn: Callable[..., Any], *args: Any) -> Resultado:
    """Ejecuta fn(*args) y convierte un error de datos en Err.

    Pre: `fn` es una función pura.
    Post: regresa Ok(resultado) o Err(mensaje); nunca lanza ValueError,
          KeyError ni TypeError.
    """
    try:
        return Ok(fn(*args))
    except (ValueError, KeyError, TypeError) as exc:
        return Err(f"{type(exc).__name__}: {exc}")


def separar(resultados: Iterable[Resultado]) -> Tuple[Tuple[Any, ...], Tuple[str, ...]]:
    """Separa resultados en (valores de los Ok, mensajes de los Err).

    Post: regresa dos tuplas nuevas; conserva el orden original.
    """
    lista = tuple(resultados)
    valores = tuple(r.valor for r in lista if r.es_ok)
    errores = tuple(r.error for r in lista if not r.es_ok)
    return valores, errores
