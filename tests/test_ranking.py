
from src.ranking import top_k


def test_top_k_con_tres_palabras():
    conteos = [("hola", 1), ("mundo", 3), ("datos", 2)]
    assert top_k(conteos, 2) == [("mundo", 3), ("datos", 2)]


def test_top_k_no_modifica_la_entrada():
    conteos = [("hola", 1), ("mundo", 3), ("datos", 2)]
    top_k(conteos, 2)
    assert conteos == [("hola", 1), ("mundo", 3), ("datos", 2)]


def test_top_k_con_k_mayor_que_los_datos():
    assert top_k([("a", 1)], 5) == [("a", 1)]