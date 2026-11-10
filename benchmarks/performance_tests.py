import time

from src.mapreduce import MapReduce
from src.monoid import SumMonoid


# Funciones de nivel de módulo: se pueden enviar a otros procesos (pickle).
def split_line(line):
    return line.split()


def to_pairs(words):
    return [(w, 1) for w in words]


def main():
    print("Benchmark: Conteo de palabras con 1 millón de líneas...")
    data = ["palabra uno dos uno" for _ in range(1_000_000)]
    for workers in (1, 2, 4):
        mr = MapReduce.of(data, workers=workers)
        start = time.time()
        result = (
            mr
            .map(split_line)
            .flat_map(to_pairs)
            .reduce_by_key(SumMonoid)
            .sort_by_value(descending=True)
            .collect()
        )
        elapsed = time.time() - start
        print(f"workers={workers}: {elapsed:.2f} s   Top: {result[:3]}")


if __name__ == "__main__":
    main()
