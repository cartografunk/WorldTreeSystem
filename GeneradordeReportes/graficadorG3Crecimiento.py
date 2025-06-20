# GeneradordeReportes/graficadorG3Crecimiento.py

from core.libs import plt, rcParams, os, pd
import numpy as np
from GeneradordeReportes.utils.db import get_engine
from GeneradordeReportes.utils.colors import COLOR_PALETTE
from GeneradordeReportes.utils.config import BASE_DIR, EXPORT_DPI
from GeneradordeReportes.utils.plot import FIGSIZE, _print_size_cm
from GeneradordeReportes.utils.helpers import (
    get_inventory_table_name,
    resolve_column
)
from GeneradordeReportes.utils.crecimiento_esperado import df_dbh
from GeneradordeReportes.utils.helpers import tiene_datos_campo, get_region_language
from GeneradordeReportes.utils.text_templates import text_templates


def generar_crecimiento(contract_code: str, country: str, year: int,
                        engine=None,
                        output_root: str = os.path.join(BASE_DIR, "GeneradordeReportes", "outputs")):

    # 1) Setup
    year = int(year)
    engine = engine or get_engine()
    inv_table = get_inventory_table_name(country, year)

    # 2) Resolver columnas
    plot_col  = resolve_column(engine, inv_table, "plot")
    dbh_col   = resolve_column(engine, inv_table, "DBH (in)")
    if not tiene_datos_campo(engine, inv_table, contract_code, dbh_col):
        print(f"⚠️ Sin datos de DBH para {contract_code}.")
        return None
    code_col  = resolve_column(engine, inv_table, "contractcode")

    # 3) Leer DBH por parcela (filtrando outliers)
    sql = f'''
        SELECT "{plot_col}" AS plot, "{dbh_col}" AS dbh
        FROM public.{inv_table}
        WHERE "{code_col}" = %(code)s
          AND "{dbh_col}" IS NOT NULL
          AND "{dbh_col}" BETWEEN 1 AND 50
    '''

    df = pd.read_sql(sql, engine, params={"code": contract_code})

    if df.empty:
        print(f"⚠️ Sin datos de DBH para {contract_code}.")
        return

    # 4) Año de plantación y edad
    meta = "masterdatabase.contract_tree_information"
    mc = resolve_column(engine, meta, "contractcode")
    py = resolve_column(engine, meta, "planting_year")
    plant = pd.read_sql(f'''
        SELECT "{py}" AS planting_year
        FROM {meta}
        WHERE "{mc}" = %(code)s
    ''', engine, params={"code": contract_code})

    if plant.empty or plant.iloc[0,0] is None:
        print(f"⚠️ No hay año de plantación para {contract_code}.")
        return
    age = year - int(plant.iloc[0,0])

    # 5) Valores esperados de DBH
    ref = df_dbh[df_dbh["Año"] == age]
    if ref.empty:
        print(f"⚠️ No hay referencia de DBH para edad {age}.")
        return
    exp_min   = float(ref["Min"].iloc[0])
    exp_ideal = float(ref["Ideal"].iloc[0])
    exp_max   = float(ref["Max"].iloc[0])

    # 6) Agrupar por plot y media de DBH
    grp = (
        df.groupby("plot")["dbh"]
          .mean()
          .reset_index(name="dbh_mean")
          .sort_values("dbh_mean")
    )
    plots = grp["plot"].astype(str).tolist()
    x     = np.arange(len(plots))

    lang = get_region_language(country)
    title = text_templates["chart_titles"]["growth"][lang].format(code=contract_code)
    ylabel = text_templates["chart_axes"]["growth_y"][lang]
    legend = text_templates["growth_legend"]

    # 7) Plot de barras
    rcParams.update({"figure.autolayout": True})
    fig, ax = plt.subplots(figsize=FIGSIZE)
    ax.bar(x, grp["dbh_mean"], 0.6,
           color=COLOR_PALETTE["secondary_green"], label=legend["mean"][lang])
    ax.hlines(exp_min, -0.5, len(x) - 0.5, linestyle="--",
              color=COLOR_PALETTE["primary_blue"], label=legend["min"][lang])
    ax.hlines(exp_ideal, -0.5, len(x) - 0.5, linestyle="-.",
              color=COLOR_PALETTE["primary_blue"], label=legend["ideal"][lang])
    ax.hlines(exp_max, -0.5, len(x) - 0.5, linestyle=":",
              color=COLOR_PALETTE["primary_blue"], label=legend["max"][lang])

    ax.set_title(title, fontsize=11, color=COLOR_PALETTE["primary_blue"])
    ax.set_ylabel("DAP (cm)", fontsize=9)
    ax.set_xticks(x)
    ax.set_xticklabels('')
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    ax.legend(loc='center left', bbox_to_anchor=(1.02, 0.5), borderaxespad=0, frameon=False, fontsize=6)

    # 8) Guardar
    out_dir = os.path.join(output_root, contract_code, "Resumen")
    os.makedirs(out_dir, exist_ok=True)
    out_png = os.path.join(out_dir, f"G3_Crecimiento_{contract_code}.png")
    _print_size_cm(fig)
    fig.savefig(out_png, dpi=EXPORT_DPI, facecolor=None)
    plt.close(fig)

    print(f"🌱 Gráfico de crecimiento (DBH) guardado: {out_png}")
    return out_png
