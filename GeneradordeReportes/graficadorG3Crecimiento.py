from GeneradordeReportes.utils.libs import pd, os
from GeneradordeReportes.utils.db import get_engine
from GeneradordeReportes.utils.plot import save_growth_candle_chart
from GeneradordeReportes.utils.crecimiento_esperado import df_referencia
from datetime import datetime

def generar_crecimiento(contract_code: str, output_root: str = "outputs"):
    engine = get_engine()

    resumen_dir = os.path.join(output_root, contract_code, "Resumen")
    os.makedirs(resumen_dir, exist_ok=True)

    # === 1. Año de siembra y edad ===
    query_edad = f"""
    SELECT DISTINCT "Año de Siembra"
    FROM public.cat_cr_inventory2025
    WHERE "id_contract" = '{contract_code}' AND "Año de Siembra" IS NOT NULL
    """
    df_siembra = pd.read_sql(query_edad, engine)

    if df_siembra.empty:
        print(f"⚠️ No hay año de siembra para {contract_code}.")
        return

    año_siembra = df_siembra["Año de Siembra"].iloc[0]
    edad = datetime.now().year - año_siembra

    # === 2. Datos esperados ===
    fila_ref = df_referencia[df_referencia["Año"] == edad]

    if fila_ref.empty:
        print(f"⚠️ No hay datos de crecimiento esperado para edad {edad} años.")
        return

    expected = {
        "Min": fila_ref["Min"].values[0],
        "Max": fila_ref["Max"].values[0],
        "Ideal": fila_ref["Ideal"].values[0]
    }

    # === 3. Datos reales (DBH (in)) ===
    df_dbh = pd.read_sql(f"""
        SELECT "DBH (in)"
        FROM public.cr_inventory_2025
        WHERE "Contract Code" = '{contract_code}' AND "DBH (in)" IS NOT NULL
    """, engine)

    if df_dbh.empty:
        print(f"⚠️ No hay datos de DBH (in) para contrato {contract_code}.")
        return

    distribucion = df_dbh["DBH (in)"].tolist()

    actual = {
        "Distribucion": distribucion,
        "Min": min(distribucion),
        "Max": max(distribucion),
        "Ideal": sum(distribucion) / len(distribucion)
    }

    # === 4. Guardar gráfico comparativo ===
    output_file = os.path.join(resumen_dir, f"G3_Crecimiento_{contract_code}.png")
    save_growth_candle_chart(expected, actual, output_path=output_file, title=f"Crecimiento - {contract_code}")
    print(f"🌱 {contract_code} guardado en {output_file}")



# Ejecutar todos los contratos
if __name__ == "__main__":
    engine = get_engine()
    contracts_df = pd.read_sql('SELECT DISTINCT "id_contract" FROM public.cat_cr_inventory2025', engine)
    for code in contracts_df["id_contract"]:
        generar_crecimiento(code)
