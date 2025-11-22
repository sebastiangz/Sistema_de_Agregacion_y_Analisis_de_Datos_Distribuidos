from typing import List, Tuple
import matplotlib.pyplot as plt


def print_top(results: List[Tuple[str, int]], n: int = 10) -> None:
    for key, value in results[:n]:
        print(f"{key}: {value}")


def plot_top_bar(results: List[Tuple[str, int]], n: int = 10, title: str = "Top resultados"):
    top = results[:n]
    labels = [k for k, _ in top]
    values = [v for _, v in top]

    plt.figure(figsize=(8, 4))
    plt.bar(labels, values)
    plt.title(title)
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.show()

