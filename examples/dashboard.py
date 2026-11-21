"""Dashboard de ventas en consola (Equipo 7).

Junta las ventas de varias sucursales y las resume con los Monoids, los
agregadores y las funciones puras del proyecto:
    python examples/dashboard.py
"""
import csv
import os
import sys
from itertools import chain
from operator import itemgetter

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.aggregators import (avg_aggregator, compose_aggregators, count_aggregator,
                             max_aggregator, min_aggregator, monoid_aggregator)
from src.funcional import pipe
from src.mapreduce import MapReduce
from src.metricas import porcentaje
from src.monoid import SumMonoid
from src.ranking import top_k
from src.resultado import Err, intentar, separar

CARPETA = os.path.join(os.path.dirname(__file__), "..", "data", "samples")
FUENTES = (
    ("centro", os.path.join(CARPETA, "sample_sales.csv")),
    ("norte", os.path.join(CARPETA, "sample_sales_norte.csv")),
)

RESUMEN = compose_aggregators(
    total=monoid_aggregator(SumMonoid, "amount"),
    ventas=count_aggregator(),
    promedio=avg_aggregator("amount"),
    minimo=min_aggregator("amount"),
    maximo=max_aggregator("amount"),
)


def _venta(fila, fuente):
    """Diccionario nuevo con el monto como número y el nombre de la fuente."""
    return {**fila, "amount": float(fila["amount"]), "fuente": fuente}


def convertir_fila(fila, fuente="?"):
    """Convierte una fila del CSV en venta sin lanzar excepciones.

    Pre: `fila` es un diccionario con la columna `amount`.
    Post: Ok(venta nueva) si el monto es un número; si no, Err con la
          fuente y el id de la fila. `fila` no se modifica.
    """
    resultado = intentar(_venta, fila, fuente)
    if resultado.es_ok:
        return resultado
    return Err(f"{fuente}, fila {fila.get('id', '?')}: {resultado.error}")


def leer_fuente(fuente, ruta):
    """Único lugar con efectos: lee un CSV y regresa una tupla de Ok/Err."""
    with open(ruta, encoding="utf-8", newline="") as f:
        return tuple(map(lambda fila: convertir_fila(fila, fuente), csv.DictReader(f)))


def leer_fuentes(fuentes):
    """Junta varias fuentes (nombre, ruta) en una sola colección.

    Post: (ventas válidas, mensajes de error). Una fila mala no detiene la
          lectura de las demás.
    """
    return separar(chain.from_iterable(map(lambda par: leer_fuente(*par), fuentes)))


def leer_ventas(ruta):
    """Lee una sola fuente y regresa solo las ventas válidas."""
    return leer_fuentes((("ventas", ruta),))[0]


def agrupar(ventas, clave):
    """((valor de la clave, tupla de ventas), ...) usando MapReduce."""
    return MapReduce.of(ventas, workers=1).key_by(clave).group_by_key().collect()


def resumen_por_categoria(ventas):
    """(categoría, resumen) de cada categoría."""
    return pipe(
        agrupar(ventas, itemgetter("category")),
        lambda grupos: tuple((categoria, RESUMEN(items)) for categoria, items in grupos),
    )


def totales_por_fuente(ventas):
    """(fuente, total) de cada fuente, sumando con el Monoid de la suma."""
    total = monoid_aggregator(SumMonoid, "amount")
    grupos = agrupar(ventas, lambda v: v.get("fuente", "?"))
    return tuple((fuente, total.aggregate(items)) for fuente, items in grupos)


def barra(valor, maximo, ancho=20):
    """Barra de texto proporcional al valor."""
    return "#" * round(ancho * valor / maximo) if maximo else ""


def renglon(categoria, r, total_general, maximo):
    """Una línea del dashboard."""
    return (f"{categoria:<9} {barra(r['total'], maximo):<20} ${r['total']:>7.2f} "
            f"{porcentaje(r['total'], total_general):>5.1f}%  ventas: {r['ventas']}  "
            f"prom: ${r['promedio']:.2f}  min: ${r['minimo']:.2f}  max: ${r['maximo']:.2f}")


def lineas_de_fuentes(ventas, errores):
    """Renglones con el total por fuente y las filas que no se pudieron leer."""
    por_fuente = "Por fuente: " + ", ".join(f"{f} ${t:.2f}" for f, t in totales_por_fuente(ventas))
    return [por_fuente] + [f"Fila ignorada ({e})" for e in errores]


def dashboard(ventas, errores=()):
    """Texto completo del dashboard. Es pura: no imprime ni lee archivos.

    Pre: `ventas` son diccionarios con `category` y `amount` numérico.
    Post: regresa el texto; `ventas` y `errores` no se modifican.
    """
    resumen = resumen_por_categoria(ventas)
    general = RESUMEN(ventas)
    totales = tuple((categoria, r["total"]) for categoria, r in resumen)
    maximo = max(map(itemgetter(1), totales), default=0)
    lineas = [f"DASHBOARD DE VENTAS · {general['ventas']} ventas · total ${general['total']:.2f}", "-" * 98]
    lineas += [renglon(c, r, general["total"], maximo) for c, r in sorted(resumen, key=lambda x: -x[1]["total"])]
    lineas += ["-" * 98] + lineas_de_fuentes(ventas, errores)
    lineas += ["Top 3 categorías: " + ", ".join(f"{c} (${t:.2f})" for c, t in top_k(totales, 3))]
    return "\n".join(lineas)


if __name__ == "__main__":
    print(dashboard(*leer_fuentes(FUENTES)))
