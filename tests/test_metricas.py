from src.metricas import porcentaje

def test_porcentaje_basico():
    assert porcentaje(2, 5) == 40.0

def test_porcentaje_total_cero_no_falla():
    assert porcentaje(3, 0) == 0.0

def test_porcentajes_de_todas_las_partes_suman_100():
    conteos = (2, 2, 1)
    total = sum(conteos)
    assert round(sum(map(lambda c: porcentaje(c, total), conteos))) == 100