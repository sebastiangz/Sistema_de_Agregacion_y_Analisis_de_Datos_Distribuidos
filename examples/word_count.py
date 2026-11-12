import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.mapreduce import MapReduce
from src.monoid import SumMonoid
from src.visualization import print_top

def read_lines(path: str):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            yield line

# Las funciones se definen fuera de main() para que Python pueda
# enviarlas a otros procesos cuando el trabajo se hace en paralelo.
def line_to_words(line):
    return line.strip().lower().split()

def word_to_pair(words):
    return [(w, 1) for w in words if w]

def main():
    text_path = os.path.join("data", "samples", "sample_text.txt")
    lines = list(read_lines(text_path))

    mr = MapReduce().load(lines)

    result = (
        mr
        .map(line_to_words) # Map: cada línea se vuelve su lista de palabras
        .flat_map(word_to_pair) # cada palabra se vuelve el par (palabra, 1)
        .reduce_by_key(SumMonoid) # Reduce: suma los 1 de cada palabra
        .sort_by_value(descending=True) # de la más repetida a la menos repetida
        .collect() # aquí se ejecuta todo y se obtiene la lista
    )

    print(f"Top 20 palabras ({len(result)} palabras distintas en total)")
    print("-" * 40)
    print_top(result, n=20)

if __name__ == "__main__":
    main()