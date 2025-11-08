from mapreduce import MapReduce

def main():
    data = ["hola mundo", "hola amigo amigo"]

    mr = MapReduce()
    result = (mr
        .load(data)
        .map(lambda line: line.split())
        .flat_map(lambda words: [(w, 1) for w in words])
        .reduce_by_key()
        .collect()
    )

    print("Resultado final:")
    print(result)

if __name__ == "__main__":
    main()
