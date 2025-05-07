from GeneradordeReportes.utils.libs import pd, os
from GeneradordeReportes.utils.db import get_engine
from GeneradordeReportes.utils.colors import COLOR_PALETTE
from GeneradordeReportes.utils.plot import save_bar_chart
from GeneradordeReportes.utils.helpers import get_inventory_table_name, get_sql_column, get_region_language
from GeneradordeReportes.utils.text_templates import text_templates
from GeneradordeReportes.utils.config import EXPORT_WIDTH_INCHES, EXPORT_HEIGHT_INCHES
from GeneradordeReportes.utils.config import BASE_DIR

def generar_altura(contract_code: str, country: str, year: int, engine, output_root: str = os.path.join(BASE_DIR, "GeneradordeReportes", "outputs")):
    engine = get_engine()
    resumen_dir = os.path.join(output_root, contract_code, "Resumen")
    os.makedirs(resumen_dir, exist_ok=True)

    table_name = get_inventory_table_name(country, year)

    # Columnas dinámicas
    plot_col   = get_sql_column("plot")
    tht_col    = get_sql_column("tht_ft")
    merch_col  = get_sql_column("merch_ht_ft")
    contract_col = get_sql_column("contractcode")

    query = f"""
    SELECT "{plot_col}" AS plot, "{tht_col}" AS tht, "{merch_col}" AS merch
    FROM public.{table_name}
    WHERE "{contract_col}" = '{contract_code}'
    """
    df_alt = pd.read_sql(query, engine)

    if df_alt.empty:
        print(f"⚠️ No hay datos de altura para contrato {contract_code}.")
        return None

    df_grouped = df_alt.groupby("plot", dropna=True).agg({
        "tht": "mean",
        "merch": "mean"
    }).dropna().reset_index()

    if df_grouped.empty:
        print(f"⚠️ No hay datos válidos de altura en {contract_code}.")
        return None

    plots             = df_grouped["plot"].astype(str).tolist()
    altura_total      = df_grouped["tht"].tolist()
    altura_comercial  = df_grouped["merch"].tolist()
    resumen_file = os.path.join(resumen_dir, f"G2_Altura_{contract_code}.png")

    if os.path.exists(resumen_file):
        print(f"⚠️ Ya existe: {resumen_file}")
    else:
        lang   = get_region_language(country)
        title  = text_templates["chart_titles"]["height"][lang].format(code=contract_code)
        xlabel = text_templates["chart_axes"]["height_x"][lang]
        ylabel = text_templates["chart_axes"]["height_y"][lang]
        keys = text_templates.get("chart_series", {}).get("height", {}).get(
            lang,
            ["Altura Total", "Altura Comercial"]  # Valores por defecto
        )
        vals   = [altura_total, altura_comercial]
        series = dict(zip(keys, vals))

        save_bar_chart(
            x_labels=plots,
            series=series,
            title=title,
            output_path=resumen_file,
            xlabel=xlabel,
            ylabel=ylabel,
            colors=[COLOR_PALETTE['primary_blue'], COLOR_PALETTE['accent_yellow']],
            figsize=(EXPORT_WIDTH_INCHES, EXPORT_HEIGHT_INCHES)
        )
        print(f"📊 Gráfico resumen de altura guardado: {resumen_file}")

    return resumen_file
