from GeneradordeReportes.utils.db import get_engine
from GeneradordeReportes.utils.colors import COLOR_PALETTE
from GeneradordeReportes.utils.plot import save_pie_chart
from GeneradordeReportes.utils.libs import pd
import os

def generar_mortalidad(contract_code: str, output_root: str = "outputs"):
    engine = get_engine()

    # Crear carpetas
    plots_dir = os.path.join(output_root, contract_code, "Mortalidad")
    resumen_dir = os.path.join(output_root, contract_code, "Resumen")
    os.makedirs(plots_dir, exist_ok=True)
    os.makedirs(resumen_dir, exist_ok=True)

    # Query
    query = f"""
    SELECT "Plot#", SUM("alive_tree") AS "Vivos", SUM("dead_tree") AS "Muertos"
    FROM public.cr_inventory_2025
    WHERE "Contract Code" = '{contract_code}'
    GROUP BY "Plot#"
    ORDER BY "Plot#"
    """
    df = pd.read_sql(query, engine)

    if df.empty:
        print(f"⚠️ No hay datos de mortalidad para contrato {contract_code}.")
        return

    vivos_total = muertos_total = 0

    for _, row in df.iterrows():
        plot = int(row["Plot#"])
        vivos = row["Vivos"]
        muertos = row["Muertos"]
        vivos_total += vivos
        muertos_total += muertos

        if vivos + muertos == 0:
            continue

        output_file = os.path.join(plots_dir, f"{contract_code}_Mortality_P{plot:02d}.png")
        if not os.path.exists(output_file):
            save_pie_chart(
                values=[vivos, muertos],
                labels=["Árboles Vivos", "Árboles Muertos"],
                title=f"Mortality - Parcela {plot}",
                output_path=output_file,
                colors=[COLOR_PALETTE['secondary_green'], COLOR_PALETTE['primary_blue']],
                fontsize=12,
                figsize=(6, 6),
                show_preview=True,
                smart_labels=True
            )
            print(f"✅ Parcela {plot} guardada.")
        else:
            print(f"⚠️ Ya existe: {output_file}")

    # Gráfico resumen
    resumen_file = os.path.join(resumen_dir, f"G1_Mortality_{contract_code}.png")
    if not os.path.exists(resumen_file):
        save_pie_chart(
            values=[vivos_total, muertos_total],
            labels=["Árboles Vivos", "Árboles Muertos"],
            title=f"Mortality - {contract_code}",
            output_path=resumen_file,
            colors=[COLOR_PALETTE['secondary_green'], COLOR_PALETTE['primary_blue']],
            figsize=(7, 7),
            fontsize=13,
            show_preview=True,
            smart_labels=True
        )
        print(f"📊 Gráfico resumen guardado: {resumen_file}")
    else:
        print(f"⚠️ Ya existe: {resumen_file}")

# Ejecutar todos los contratos si se corre directamente
if __name__ == "__main__":
    engine = get_engine()
    contracts_df = pd.read_sql(
        'SELECT DISTINCT "id_contract" FROM public.cat_cr_inventory2025 ORDER BY "id_contract"', engine
    )
    for code in contracts_df["id_contract"]:
        generar_mortalidad(code)
