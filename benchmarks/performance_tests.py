import time
from src.mapreduce import MapReduce

def main():
    print("Benchmark: Conteo de palabras con 1 millón de líneas...")
    data = ["palabra uno dos uno" for _ in range(1_000_000)]
    mr = MapReduce().load(data)
    start = time.time()
    result = (
        mr
        .map(lambda line: line.split())
        .flat_map(lambda ws: [(w, 1) for w in ws])
        .reduce_by_key(lambda a, b: a + b)
        .sort_by_value(descending=True)
        .collect()
    )
    elapsed = time.time() - start
    print(f"Tiempo transcurrido: {elapsed:.2f} s")
    print("Top:", result[:5])

if __name__ == "__main__":
    main()

