import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import csv
from src.mapreduce import MapReduce
from src.monoid import SumMonoid
from src.metricas import porcentaje

def read_logs(path: str):
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row

def row_to_level(row):
    return (row.get("level", "UNKNOWN"), 1)

def contar_niveles(rows):
    """Cuenta los registros de cada nivel (ERROR, INFO...) con Map-Reduce.

    Post: lista nueva de pares (nivel, cantidad), de mayor a menor.
    """
    return (
        MapReduce().load(rows)
        .map(row_to_level)
        .reduce_by_key(SumMonoid)
        .sort_by_value(descending=True)
        .collect()
    )


def reporte(result):
    """Texto con el total y el porcentaje de cada nivel. Es pura: no imprime."""
    total = sum(count for _, count in result)
    lineas = [f"{level:<8} {count:>3}  ({porcentaje(count, total):.0f}%)" for level, count in result]
    return "\n".join([f"Registros analizados: {total}"] + lineas)


def main():
    log_path = os.path.join("data", "samples", "sample_logs.csv")
    print(reporte(contar_niveles(read_logs(log_path))))


if __name__ == "__main__":
    main()

