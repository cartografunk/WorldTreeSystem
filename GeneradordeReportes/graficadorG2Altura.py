from GeneradordeReportes.utils.libs import plt, rcParams, os, pd
import numpy as np
from GeneradordeReportes.utils.db import get_engine
from GeneradordeReportes.utils.colors import COLOR_PALETTE
from GeneradordeReportes.utils.config import BASE_DIR, EXPORT_DPI
from GeneradordeReportes.utils.plot import FIGSIZE, _print_size_cm
from GeneradordeReportes.utils.helpers import (
    get_inventory_table_name,
    get_region_language,
    resolve_column
)
from GeneradordeReportes.utils.text_templates import text_templates
from GeneradordeReportes.utils.crecimiento_esperado import df_altura  # DataFrame con columnas Año, Min, Max


def generar_altura(contract_code: str, country: str, year: int, engine=None, output_root: str = os.path.join(BASE_DIR, "GeneradordeReportes", "outputs")):
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

    # 3) Leer datos de inventario
    sql = f"""
        SELECT
          "{plot_col}" AS plot,
          "{tht_col}"  AS tht,
          "{mht_col}"  AS mht
        FROM public.{inv_table}
        WHERE "{code_col}" = %(code)s
    """
    df = pd.read_sql(sql, engine, params={"code": contract_code})
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
    if expected.empty:
        print(f"⚠️ No hay valores esperados para la edad {age}.")
        return
    exp_min = expected["Min"].iloc[0]
    exp_max = expected["Max"].iloc[0]

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
    lang       = get_region_language(country)
    title      = text_templates["chart_titles"]["height"][lang].format(code=contract_code)
    ylabel     = text_templates["chart_axes"]["height_y"][lang]
    resumen_dir = os.path.join(output_root, contract_code, "Resumen")
    os.makedirs(resumen_dir, exist_ok=True)
    out_png     = os.path.join(resumen_dir, f"G2_Altura_{contract_code}.png")
    if os.path.exists(out_png):
        print(f"⚠️ Ya existe: {out_png}")
        return out_png

    # 8) Plot de barras agrupadas
    rcParams.update({"figure.autolayout": True})
    fig, ax = plt.subplots(figsize=FIGSIZE)
    ax.bar(x - w/2, df_group["tht_mean"], width=w, label="THT_promedio (ft)", color=COLOR_PALETTE['primary_blue'], alpha=0.8)
    ax.bar(x + w/2, df_group["mht_mean"], width=w, label="MHT_promedio (ft)", color=COLOR_PALETTE['secondary_green'], alpha=0.8)

    # 9) Líneas horizontales esperadas
    ax.hlines(exp_min, xmin=-w, xmax=n-1 + w, linestyles='--', color=COLOR_PALETTE['accent_yellow'], label="Mínimo esperado")
    ax.hlines(exp_max, xmin=-w, xmax=n-1 + w, linestyles=':',  color=COLOR_PALETTE['secondary_green'], label="Máximo esperado")

    # 10) Configuración de ejes
    ax.set_title(title, fontsize=11, color=COLOR_PALETTE['primary_blue'])
    ax.set_ylabel(ylabel, fontsize=9)
    ax.set_xlable("Parcelas", fontsize=9)
    ax.set_xticks(x)
    ax.set_xticklabels("")
    ax.grid(axis='y', linestyle='--', alpha=0.3)
    ax.legend()

    # 11) Guardar figura
    _print_size_cm(fig)
    fig.savefig(out_png, dpi=EXPORT_DPI, facecolor=None)
    plt.close(fig)
    print(f"📊 Bar chart agrupado de alturas guardado: {out_png}")
    return out_png
