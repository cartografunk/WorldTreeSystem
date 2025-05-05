from GeneradordeReportes.utils.db import get_engine
from GeneradordeReportes.utils.colors import COLOR_PALETTE
from GeneradordeReportes.utils.plot import save_pie_chart
from GeneradordeReportes.utils.libs import pd, os
from GeneradordeReportes.utils.helpers import get_inventory_table_name

def generar_mortalidad(contract_code: str, country: str, year: int, output_root: str = "outputs"):
    engine = get_engine()
    resumen_dir = os.path.join(output_root, contract_code, "Resumen")
    os.makedirs(resumen_dir, exist_ok=True)

    table_name = get_inventory_table_name(country, year)

    query = f"""
    SELECT SUM("alive_tree") AS "Vivos", SUM("dead_tree") AS "Muertos"
    FROM public.{table_name}
    WHERE "Contract Code" = '{contract_code}'
    """
    df = pd.read_sql(query, engine)

    if df.empty or (df["Vivos"].iloc[0] is None and df["Muertos"].iloc[0] is None):
        print(f"⚠️ No hay datos de mortalidad para contrato {contract_code}.")
        return

    vivos = df["Vivos"].iloc[0] or 0
    muertos = df["Muertos"].iloc[0] or 0

    resumen_file = os.path.join(resumen_dir, f"G1_Mortality_{contract_code}.png")
    if not os.path.exists(resumen_file):
        save_pie_chart(
            values=[muertos, vivos],
            labels=["Muertos", "Vivos"],
            title=f"Mortalidad - {contract_code}",
            output_path=resumen_file,
            colors=[COLOR_PALETTE['primary_blue'], COLOR_PALETTE['secondary_green']],
            figsize=(7, 7),
            fontsize=13
        )
        print(f"📊 Gráfico resumen guardado: {resumen_file}")
    else:
        print(f"⚠️ Ya existe: {resumen_file}")
