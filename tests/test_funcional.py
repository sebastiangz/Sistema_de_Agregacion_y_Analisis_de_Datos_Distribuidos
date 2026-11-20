from src.funcional import compose, identidad, pipe


def sumar_uno(x):
    return x + 1


def doble(x):
    return x * 2


def test_compose_aplica_de_derecha_a_izquierda():
    assert compose(sumar_uno, doble)(5) == 11  # sumar_uno(doble(5))


def test_pipe_aplica_de_izquierda_a_derecha():
    assert pipe(5, sumar_uno, doble) == 12  # doble(sumar_uno(5))


def test_compose_sin_funciones_es_identidad():
    assert compose()(7) == 7 == identidad(7)


def test_compose_es_asociativa():
    izquierda = compose(compose(sumar_uno, doble), sumar_uno)
    derecha = compose(sumar_uno, compose(doble, sumar_uno))
    assert all(map(lambda x: izquierda(x) == derecha(x), range(-5, 6)))


def test_pipe_no_modifica_la_entrada():
    datos = (3, 1, 2)
    assert pipe(datos, sorted, tuple) == (1, 2, 3)
    assert datos == (3, 1, 2)
