from examples.dashboard import dashboard, resumen_por_categoria
VENTAS = (
    {"category": "Bebidas", "amount": 30.0},
    {"category": "Comida", "amount": 100.0},
    {"category": "Bebidas", "amount": 20.0},
)
def test_resumen_por_categoria():
    resumen = dict(resumen_por_categoria(VENTAS))
    assert resumen["Bebidas"]["total"] == 50.0
    assert resumen["Bebidas"]["ventas"] == 2
    assert resumen["Comida"]["maximo"] == 100.0
def test_dashboard_muestra_total_y_top():
    texto = dashboard(VENTAS)
    assert "3 ventas · total $150.00" in texto
    assert texto.splitlines()[-1] == "Top 3 categorías: Comida ($100.00), Bebidas ($50.00)"
def test_dashboard_no_modifica_las_ventas():
    copia = tuple(dict(v) for v in VENTAS)
    dashboard(VENTAS)
    assert VENTAS == copia

def test_convertir_fila_invalida_regresa_err():
    from examples.dashboard import convertir_fila
    fila = {"id": "N6", "category": "Bebidas", "amount": "N/A"}
    resultado = convertir_fila(fila, "norte")
    assert not resultado.es_ok
    assert resultado.error.startswith("norte, fila N6")
    assert fila["amount"] == "N/A"


def test_leer_fuentes_junta_varias_y_separa_errores(tmp_path):
    from examples.dashboard import leer_fuentes
    (tmp_path / "a.csv").write_text("id,category,amount\n1,Bebidas,10\n", encoding="utf-8")
    (tmp_path / "b.csv").write_text("id,category,amount\n2,Comida,5\n3,Comida,x\n", encoding="utf-8")
    ventas, errores = leer_fuentes((("a", tmp_path / "a.csv"), ("b", tmp_path / "b.csv")))
    assert tuple(v["fuente"] for v in ventas) == ("a", "b")
    assert len(errores) == 1 and errores[0].startswith("b, fila 3")


def test_dashboard_reporta_errores_y_fuentes():
    from examples.dashboard import dashboard
    ventas = ({"category": "Bebidas", "amount": 10.0, "fuente": "centro"},)
    texto = dashboard(ventas, ("norte, fila N6: ValueError",))
    assert "Por fuente: centro $10.00" in texto
    assert "Fila ignorada (norte, fila N6: ValueError)" in texto
