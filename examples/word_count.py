import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.mapreduce import MapReduce
from src.visualization import print_top

def read_lines(path: str):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            yield line

# --- CAMBIO CLAVE: define funciones fuera de main ---
def line_to_words(line):
    return line.strip().lower().split()

def word_to_pair(words):
    return [(w, 1) for w in words if w]

def main():
    text_path = os.path.join("data", "sample", "sample_text.txt")
    lines = list(read_lines(text_path))

    mr = MapReduce().load(lines)

    result = (
        mr
        .map(line_to_words)
        .flat_map(word_to_pair)
        .reduce_by_key(lambda a, b: a + b)
        .sort_by_value(descending=True)
        .collect()
    )

    print_top(result, n=20)

if __name__ == "__main__":
    main()

