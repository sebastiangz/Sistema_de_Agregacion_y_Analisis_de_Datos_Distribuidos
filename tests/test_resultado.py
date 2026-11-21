import pytest

from src.resultado import Err, Ok, intentar, separar


def test_ok_map_y_bind():
    assert Ok(2).map(lambda x: x * 10) == Ok(20)
    assert Ok(2).bind(lambda x: Ok(x + 1)) == Ok(3)


def test_err_corta_la_cadena():
    error = Err("dato inválido")
    assert error.map(lambda x: x * 10) == error
    assert error.bind(lambda x: Ok(x + 1)) == error


def test_intentar_convierte_excepciones_en_err():
    assert intentar(float, "12.5") == Ok(12.5)
    resultado = intentar(float, "N/A")
    assert not resultado.es_ok and resultado.error.startswith("ValueError")


def test_separar_conserva_orden():
    valores, errores = separar([Ok(1), Err("a"), Ok(2), Err("b")])
    assert valores == (1, 2) and errores == ("a", "b")


def test_resultados_son_inmutables():
    with pytest.raises(Exception):
        Ok(1).valor = 2
