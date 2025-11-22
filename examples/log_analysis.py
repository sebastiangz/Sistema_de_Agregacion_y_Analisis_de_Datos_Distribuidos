import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import csv
from src.mapreduce import MapReduce

def read_logs(path: str):
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row

def row_to_level(row):
    return (row.get("level", "UNKNOWN"), 1)

def sum_counts(a, b):
    return a + b

def main():
    log_path = os.path.join("data", "sample", "sample_logs.csv")
    rows = list(read_logs(log_path))

    mr = MapReduce().load(rows)

    result = (
        mr
        .map(row_to_level)
        .reduce_by_key(sum_counts)
        .sort_by_value(descending=True)
        .collect()
    )

    for level, count in result:
        print(level, count)

if __name__ == "__main__":
    main()

