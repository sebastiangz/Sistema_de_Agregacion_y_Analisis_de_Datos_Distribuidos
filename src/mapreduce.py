# src/mapreduce.py

class MapReduce:
    def __init__(self):
        self.data = []

    def load(self, data):
        """Cargar datos"""
        self.data = data
        return self

    def map(self, func):
        """Aplica una función a cada elemento"""
        self.data = [func(x) for x in self.data]
        return self

    def flat_map(self, func):
        """Convierte listas dentro de listas en una sola"""
        mapped = [func(x) for x in self.data]
        self.data = [item for sublist in mapped for item in sublist]
        return self

    def reduce_by_key(self):
        """Cuenta cuántas veces aparece cada palabra"""
        result = {}
        for key, value in self.data:
            result[key] = result.get(key, 0) + value
        self.data = list(result.items())
        return self

    def collect(self):
        """Devuelve el resultado"""
        return self.data
