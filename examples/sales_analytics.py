import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import csv
from src.aggregators import sum_aggregator, count_aggregator, avg_aggregator, compose_aggregators

def read_sales(path: str):
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row["amount"] = float(row.get("amount", 0.0))
            yield row

def group_by(rows, field):
    groups = {}
    for row in rows:
        key = row.get(field, "UNKNOWN")
        groups.setdefault(key, []).append(row)
    return groups

def main():
    sales_path = os.path.join("data", "sample", "sample_sales.csv")
    rows = list(read_sales(sales_path))

    groups = group_by(rows, "category")

    agg = compose_aggregators(
        total=sum_aggregator("amount"),
        count=count_aggregator(),
        avg=avg_aggregator("amount"),
    )

    for category, items in groups.items():
        stats = agg(items)
        print(category, stats)

if __name__ == "__main__":
    main()

