from GeneradordeReportes.utils.db import get_engine
from GeneradordeReportes.utils.helpers import get_region_language, get_inventory_table_name, get_sql_column
from GeneradordeReportes.utils.text_templates import text_templates
from GeneradordeReportes.utils.colors import COLOR_PALETTE
from GeneradordeReportes.utils.plot import save_pie_chart
from GeneradordeReportes.utils.libs import pd, os
from GeneradordeReportes.utils.config import EXPORT_WIDTH_INCHES, EXPORT_HEIGHT_INCHES, BASE_DIR

def generar_mortalidad(contract_code: str, country: str, year: int,
                       engine=None,
                       output_root: str = os.path.join(BASE_DIR, "GeneradordeReportes", "outputs")):
    engine = engine or get_engine()
    resumen_dir = os.path.join(output_root, contract_code, "Resumen")
    os.makedirs(resumen_dir, exist_ok=True)

    table_name = get_inventory_table_name(country, year)

    # Obtener nombres de columna correctos según el esquema
    col_code = 'contractcode' # devuelve "contractcode"
    col_alive = get_sql_column("alive_tree")
    col_dead = get_sql_column("dead_tree")

    query = f"""
    SELECT
      SUM("{col_dead}") AS muertos,
      SUM("{col_alive}") AS vivos
    FROM public.{table_name}
    WHERE {col_code} = %(code)s
    """
    df = pd.read_sql(query, engine, params={"code": contract_code})

    # Verificar datos
    if df.empty or (df.iloc[0]["vivos"] is None and df.iloc[0]["muertos"] is None):
        print(f"⚠️ No hay datos de mortalidad para contrato {contract_code}.")
        return None

    muertos = int(df.loc[0, "muertos"] or 0)
    vivos   = int(df.loc[0, "vivos"]   or 0)

    resumen_file = os.path.join(resumen_dir, f"G1_Mortality_{contract_code}.png")
    lang = get_region_language(country)
    title = text_templates["chart_titles"]["mortality"][lang].format(code=contract_code)
    labels = text_templates["chart_labels"]["mortality"][lang]

    # Generar gráfico
    save_pie_chart(
        values=[muertos, vivos],
        labels=labels,
        title=title,
        output_path=resumen_file,
        colors=[COLOR_PALETTE['primary_blue'], COLOR_PALETTE['secondary_green']],
        figsize=(EXPORT_WIDTH_INCHES, EXPORT_HEIGHT_INCHES)
    )
    print(f"📊 Gráfico de mortalidad guardado: {resumen_file}")
    return resumen_file
