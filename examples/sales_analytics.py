import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import csv
from functools import reduce
from src.aggregators import sum_aggregator, count_aggregator, avg_aggregator, compose_aggregators


def convertir_fila(row):
    """Regresa una fila NUEVA con `amount` como número.

    Pre: `row` es un diccionario leído del CSV.
    Post: `row` no se modifica; si falta `amount` se usa 0.0.
    """
    return {**row, "amount": float(row.get("amount", 0.0))}


def read_sales(path: str):
    """Único lugar con efectos: lee el CSV y regresa una tupla inmutable."""
    with open(path, "r", encoding="utf-8", newline="") as f:
        return tuple(map(convertir_fila, csv.DictReader(f)))


def agregar_a_grupo(grupos, row, field):
    """Regresa un diccionario NUEVO con `row` agregada al grupo de su clave."""
    key = row.get(field, "UNKNOWN")
    return {**grupos, key: grupos.get(key, ()) + (row,)}


def group_by(rows, field):
    """Agrupa filas por `field` con reduce, sin modificar nada.

    Post: {clave: tupla de filas}, en el orden en que aparecen las claves.
    """
    return reduce(lambda grupos, row: agregar_a_grupo(grupos, row, field), rows, {})


def main():
    sales_path = os.path.join("data", "samples", "sample_sales.csv")
    rows = read_sales(sales_path)

    groups = group_by(rows, "category")

    agg = compose_aggregators(
        total=sum_aggregator("amount"),
        count=count_aggregator(),
        avg=avg_aggregator("amount"),
    )

    print("Ventas por categoría")
    print("-" * 58)
    for category, items in groups.items():
        stats = agg(items)
        print(
            f"{category:<10} total: ${stats['total']:>8.2f} "
            f"ventas: {stats['count']} promedio: ${stats['avg']:.2f}"
        )

if __name__ == "__main__":
    main()
