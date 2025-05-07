from GeneradordeReportes.utils.libs import pd, os
from GeneradordeReportes.utils.db import get_engine
from GeneradordeReportes.utils.plot import save_growth_candle_chart
from GeneradordeReportes.utils.crecimiento_esperado import df_referencia
from GeneradordeReportes.utils.helpers import get_inventory_table_name
from datetime import datetime
from GeneradordeReportes.utils.config import BASE_DIR

def generar_crecimiento(contract_code: str, country: str, year: int, engine, output_root: str = os.path.join(BASE_DIR, "GeneradordeReportes", "outputs")):
    engine = get_engine()
    resumen_dir = os.path.join(output_root, contract_code, "Resumen")
    os.makedirs(resumen_dir, exist_ok=True)

    table_name = get_inventory_table_name(country, year)

    # 1) Año de siembra
    query_edad = f"""
    SELECT DISTINCT "Año de Siembra"
    FROM public.cat_inventory_{country}_{year}
    WHERE "id_contract" = '{contract_code}' AND "Año de Siembra" IS NOT NULL
    """
    df_siembra = pd.read_sql(query_edad, engine)
    if df_siembra.empty:
        print(f"⚠️ No hay año de siembra para {contract_code}.")
        return None

    año_siembra = df_siembra["Año de Siembra"].iloc[0]
    edad = datetime.now().year - año_siembra

    # 2) Datos esperados
    fila_ref = df_referencia[df_referencia["Año"] == edad]
    if fila_ref.empty:
        print(f"⚠️ No hay datos de crecimiento esperado para edad {edad} años.")
        return None

    expected = {
        "Min":   fila_ref["Min"].values[0],
        "Max":   fila_ref["Max"].values[0],
        "Ideal": fila_ref["Ideal"].values[0]
    }

    # 3) Datos reales (DBH)
    df_dbh = pd.read_sql(f"""
        SELECT "DBH (in)"
        FROM public.{table_name}
        WHERE "Contract Code" = '{contract_code}' AND "DBH (in)" IS NOT NULL
    """, engine)
    if df_dbh.empty:
        print(f"⚠️ No hay datos de DBH (in) para contrato {contract_code}.")
        return None

    distribucion = df_dbh["DBH (in)"].tolist()
    actual = {
        "Distribucion": distribucion,
        "Min":          min(distribucion),
        "Max":          max(distribucion),
        "Ideal":        sum(distribucion) / len(distribucion)
    }

    # 4) Guardar gráfico comparativo
    output_file = os.path.join(resumen_dir, f"G3_Crecimiento_{contract_code}.png")
    save_growth_candle_chart(
        expected, actual,
        output_path=output_file,
        title=f"Crecimiento - {contract_code}"
    )
    print(f"🌱 Gráfico de crecimiento guardado en {output_file}")

    return output_file
