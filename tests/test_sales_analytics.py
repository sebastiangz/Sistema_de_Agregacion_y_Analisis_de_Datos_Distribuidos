from examples.sales_analytics import convertir_fila, group_by

FILAS = (
    {"id": "1", "category": "Bebidas", "amount": 30.0},
    {"id": "2", "category": "Comida", "amount": 100.0},
    {"id": "3", "category": "Bebidas", "amount": 20.0},
)


def test_convertir_fila_no_modifica_la_original():
    fila = {"id": "1", "category": "Bebidas", "amount": "34.5"}
    nueva = convertir_fila(fila)
    assert nueva["amount"] == 34.5
    assert fila["amount"] == "34.5"


def test_group_by_agrupa_en_tuplas():
    grupos = group_by(FILAS, "category")
    assert tuple(f["id"] for f in grupos["Bebidas"]) == ("1", "3")
    assert len(grupos["Comida"]) == 1


def test_group_by_no_modifica_las_filas():
    copia = tuple(dict(f) for f in FILAS)
    group_by(FILAS, "category")
    assert FILAS == copia


def test_reporte_es_texto_sin_efectos():
    from examples.sales_analytics import reporte
    texto = reporte(FILAS)
    assert texto.splitlines()[0] == "Ventas por categoría"
    assert "Bebidas    total: $   50.00 ventas: 2 promedio: $25.00" in texto
