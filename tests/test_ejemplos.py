from examples.log_analysis import contar_niveles, reporte
from examples.word_count import contar_palabras


def test_contar_palabras():
    resultado = dict(contar_palabras(["hola mundo", "mundo"]))
    assert resultado == {"mundo": 2, "hola": 1}


def test_contar_niveles_y_reporte():
    filas = ({"level": "ERROR"}, {"level": "INFO"}, {"level": "ERROR"})
    conteo = contar_niveles(filas)
    assert conteo[0] == ("ERROR", 2)
    texto = reporte(conteo)
    assert texto.splitlines()[0] == "Registros analizados: 3"
    assert "ERROR      2  (67%)" in texto
