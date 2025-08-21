#ReportGenerator/graficadorG2 Altura


from core.libs import plt, rcParams, os, pd, np
from ReportGenerator.utils.db import get_engine
from ReportGenerator.utils.colors import COLOR_PALETTE
from ReportGenerator.utils.config import BASE_DIR, EXPORT_DPI
from ReportGenerator.utils.plot import FIGSIZE, _print_size_cm
from ReportGenerator.utils.helpers import (
    get_inventory_table_name,
    get_region_language,
    resolve_column
)
from ReportGenerator.utils.text_templates import text_templates
from ReportGenerator.utils.crecimiento_esperado import df_altura  # DataFrame con columnas Año, Min, Max
from ReportGenerator.utils.helpers import tiene_datos_campo

def generar_altura(contract_code: str, country: str, year: int, engine=None, output_root: str = os.path.join(BASE_DIR, "ReportGenerator", "outputs")):
    """
    Genera un bar chart donde cada barra representa el promedio de alturas THT y MHT
    por parcela (plot), más líneas horizontales de mínimo y máximo esperado según la edad.
    """
    # 1) Configuración inicial
    year = int(year)
    engine = engine or get_engine()
    inv_table = get_inventory_table_name(country, year)

    # 2) Resolver columnas: THT, MHT y plot
    tht_col   = resolve_column(engine, inv_table, "tht_ft")
    mht_col   = resolve_column(engine, inv_table, "merch_ht_ft")
    plot_col  = resolve_column(engine, inv_table, "plot")
    code_col  = resolve_column(engine, inv_table, "contractcode")

    # — Si no hay datos ni en THT ni en MHT, omitir el gráfico —
    if not (
            tiene_datos_campo(engine, inv_table, contract_code, tht_col) or
            tiene_datos_campo(engine, inv_table, contract_code, mht_col)
    ):
        print(f"⚠️ Sin datos de altura (THT/MHT) para {contract_code}.")
        return None

    # 3) Leer datos de inventario
    sql = f"""
        SELECT
          {plot_col} AS plot,
          {tht_col}  AS tht,
          {mht_col}  AS mht
        FROM public.{inv_table}
        WHERE {code_col} = %(code)s
    """

    df = pd.read_sql(sql, engine, params={"code": contract_code})

    #omitir atípicos
    df = df[
        (df["tht"].between(1, 100)) &
        (df["mht"].between(1, 100))
        ]

    if df.empty:
        print(f"⚠️ Sin datos de altura para {contract_code}.")
        return

    # 4) Obtener planting_year
    tree_table    = "masterdatabase.contract_tree_information"
    tree_code_col = resolve_column(engine, tree_table, "contractcode")
    plant_col     = resolve_column(engine, tree_table, "planting_year")
    sql_py = f"""
        SELECT "{plant_col}" AS planting_year
        FROM {tree_table}
        WHERE "{tree_code_col}" = %(code)s
    """
    plant_df = pd.read_sql(sql_py, engine, params={"code": contract_code})
    if plant_df.empty or plant_df.iloc[0,0] is None:
        print(f"⚠️ No se encontró planting_year para {contract_code}.")
        return
    planting_year = int(plant_df.iloc[0,0])

    # 5) Calcular edad y valores esperados
    age = year - planting_year
    expected = df_altura[df_altura["Año"] == age]
    has_reference = not expected.empty

    if has_reference:
        exp_min = expected["Min"].iloc[0]
        exp_max = expected["Max"].iloc[0]
    else:
        print(f"⚠️ No hay valores esperados para la edad {age}.")

    # 6) Agrupar por plot y calcular promedio
    df_group = (
        df.dropna(subset=["tht","mht"])
          .groupby("plot")
          .agg(tht_mean=("tht","mean"), mht_mean=("mht","mean"))
          .reset_index()
          .sort_values("tht_mean")
    )

    plots = df_group["plot"].tolist()
    n     = len(plots)
    x     = np.arange(n)
    w     = 0.4

    # 7) Textos y paths
    lang = get_region_language(country)
    title = text_templates["chart_titles"]["height"][lang].format(code=contract_code)
    ylabel = text_templates["chart_axes"]["height_y"][lang]
    xlabel = text_templates["chart_axes"]["height_x"][lang]
    series = text_templates["chart_series"]["height"][lang]  # ["Altura total", "Altura comercial"]
    legend_min = text_templates.get("height_legend_min", {}).get(lang, "Expected minimum")
    legend_max = text_templates.get("height_legend_max", {}).get(lang, "Expected maximum")

    resumen_dir = os.path.join(output_root, contract_code)  # <--- agrega esto AQUÍ
    os.makedirs(resumen_dir, exist_ok=True)
    out_png = os.path.join(resumen_dir, f"G2_Altura_{contract_code}.png")

    if os.path.exists(out_png):
        print(f"⚠️ Ya existe: {out_png}")
        return out_png

    # 8) Plot de barras agrupadas
    rcParams.update({"figure.autolayout": True})
    fig, ax = plt.subplots(figsize=FIGSIZE)
    ax.bar(x - w / 2, df_group["tht_mean"], width=w, label=series[0], color=COLOR_PALETTE['primary_blue'], alpha=0.8)
    ax.bar(x + w / 2, df_group["mht_mean"], width=w, label=series[1], color=COLOR_PALETTE['secondary_green'], alpha=0.8)

    # 9) Líneas horizontales esperadas (solo si hay referencia)
    if has_reference:
        ax.hlines(exp_min, xmin=-w, xmax=n - 1 + w, linestyles='--',
                  color=COLOR_PALETTE['accent_yellow'], label=legend_min)
        ax.hlines(exp_max, xmin=-w, xmax=n - 1 + w, linestyles=':',
                  color=COLOR_PALETTE['secondary_green'], label=legend_max)

    # 10) Configuración de ejes
    ax.set_title(title, fontsize=11, color=COLOR_PALETTE['primary_blue'])
    ax.set_ylabel(ylabel, fontsize=9)
    #ax.set_xlabel(xlabel, fontsize=9)
    ax.set_xticks(x)
    ax.set_xticklabels("")
    ax.grid(axis='y', linestyle='--', alpha=0.3)
    ax.legend(loc='center left', bbox_to_anchor=(1.02, 0.5), borderaxespad=0, frameon=False, fontsize=6)


    # 11) Guardar figura
    _print_size_cm(fig)
    fig.savefig(out_png, dpi=EXPORT_DPI, facecolor=None)
    plt.close(fig)
    print(f"📊 Bar chart agrupado de alturas guardado: {out_png}")
    return out_png
