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