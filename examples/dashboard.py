"""Dashboard de ventas en consola (Equipo 7).
    Usa los Monoids, los agregadores y las funciones puras del proyecto:
    python examples/dashboard.py
"""
import csv
import os
import sys
from operator import itemgetter

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.aggregators import (avg_aggregator, compose_aggregators, count_aggregator,
max_aggregator, min_aggregator, monoid_aggregator)

from src.mapreduce import MapReduce
from src.metricas import porcentaje
from src.monoid import SumMonoid
from src.ranking import top_k

RUTA = os.path.join(os.path.dirname(__file__), "..", "data", "samples", "sample_sales.csv")
RESUMEN = compose_aggregators(
    total=monoid_aggregator(SumMonoid, "amount"),
    ventas=count_aggregator(),
    promedio=avg_aggregator("amount"),
    minimo=min_aggregator("amount"),
    maximo=max_aggregator("amount"),
)

def convertir_fila(fila):
    """Fila del CSV .> venta con el monto como número (diccionario nuevo)."""
    return {**fila, "amount": float(fila["amount"])}

def leer_ventas(ruta):
    """Único lugar con efectos: lee el archivo y regresa una tupla inmutable."""
    with open(ruta, encoding="utf-8", newline="") as f:
        return tuple(map(convertir_fila, csv.DictReader(f)))

def resumen_por_categoria(ventas):
    """(categoría, resumen) de cada categoría, agrupando con MapReduce."""
    grupos = MapReduce.of(ventas, workers=1).key_by(itemgetter("category")).group_by_key().collect()
    return tuple((categoria, RESUMEN(items)) for categoria, items in grupos)

def barra(valor, maximo, ancho=20):
    """Barra de texto proporcional al valor."""
    return "#" * round(ancho * valor / maximo) if maximo else ""

def renglon(categoria, r, total_general, maximo):
    """Una línea del dashboard."""
    return (f"{categoria:<9} {barra(r['total'], maximo):<20} ${r['total']:>7.2f} "
            f"{porcentaje(r['total'], total_general):>5.1f}%  ventas: {r['ventas']}  "
            f"prom: ${r['promedio']:.2f}  min: ${r['minimo']:.2f}  max: ${r['maximo']:.2f}")
    
def dashboard(ventas):
    """Texto completo del dashboard. Es pura: no imprime ni lee archivos."""
    resumen = resumen_por_categoria(ventas)
    general = RESUMEN(ventas)
    totales = tuple((categoria, r["total"]) for categoria, r in resumen)
    maximo = max(map(itemgetter(1), totales), default=0)
    lineas = [f"DASHBOARD DE VENTAS · {general['ventas']} ventas · total ${general['total']:.2f}", "-" * 98]
    lineas += [renglon(c, r, general["total"], maximo) for c, r in sorted(resumen, key=lambda x: -x[1]["total"])]
    lineas += ["-" * 98, "Top 3 categorías: " + ", ".join(f"{c} (${t:.2f})" for c, t in top_k(totales, 3))]
    return "\n".join(lineas)
if __name__ == "__main__":
    print(dashboard(leer_ventas(RUTA)))