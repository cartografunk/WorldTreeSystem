# ReportGenerator/graficadorG3Crecimiento.py

from core.libs import plt, rcParams, os, pd, np
from core.units import UNITS_BLOCK
from ReportGenerator.utils.db import get_engine
from ReportGenerator.utils.colors import COLOR_PALETTE
from ReportGenerator.utils.config import BASE_DIR, EXPORT_DPI
from ReportGenerator.utils.plot import FIGSIZE, _print_size_cm
from ReportGenerator.utils.helpers import (
    get_inventory_table_name,
    resolve_column
)
from ReportGenerator.utils.crecimiento_esperado import df_dbh
from ReportGenerator.utils.helpers import tiene_datos_campo, get_region_language
from ReportGenerator.utils.text_templates import text_templates


def generar_crecimiento(contract_code: str, country: str, year: int,
                        engine=None,
                        output_root: str = os.path.join(BASE_DIR, "ReportGenerator", "outputs"),
                        chart_type: str = "bar"):
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
    lang = get_region_language(country)
    dbh_units = UNITS_BLOCK["dbh"][lang]  # esto elige factor y label

    # 5.1. Selecciona la referencia correcta para la edad
    ref = df_dbh[df_dbh["Año"] == age]
    if ref.empty:
        print(f"⚠️ No hay referencia de DBH para edad {age}.")
        return

    # 5.2. Aplica el factor SIEMPRE a las referencias
    exp_min = float(ref["Min"].iloc[0]) * dbh_units["factor"]
    exp_ideal = float(ref["Ideal"].iloc[0]) * dbh_units["factor"]
    exp_max = float(ref["Max"].iloc[0]) * dbh_units["factor"]

    # 6) Preparar leyendas y títulos
    title = text_templates["chart_titles"]["growth"][lang].format(code=contract_code)
    ylabel = text_templates["chart_axes"]["growth_y"][lang]
    legend = text_templates["growth_legend"]


    # ====================
    #     SCATTER ORDENADO
    # ====================
    if chart_type == "scatter" and lang == "en":
        rcParams.update({"figure.autolayout": True})
        fig, ax = plt.subplots(figsize=FIGSIZE)

        # Expected range (rectángulo)
        ax.axhspan(exp_min, exp_max, color=COLOR_PALETTE["primary_blue"], alpha=0.20, label="Expected range")
        # Línea ideal
        ax.axhline(exp_ideal, linestyle="--", color=COLOR_PALETTE["primary_blue"], label=legend["ideal"][lang])

        # Ordenar todos los valores de DBH (sin importar parcela)
        dbhs_sorted = np.sort(df["dbh"].values)
        x = np.arange(1, len(dbhs_sorted) + 1)

        # Scatter: cada árbol, solo ordenados
        ax.scatter(x, dbhs_sorted,
                   color=COLOR_PALETTE["secondary_green"],
                   edgecolor="k", linewidths=.2, alpha=0.7, s=30, label="DBH per tree")

        ax.set_title(f"DBH per tree – {contract_code}", fontsize=11, color=COLOR_PALETTE["primary_blue"])
        ax.set_ylabel(ylabel, fontsize=9)
        #ax.set_xlabel("Sample (ordered)", fontsize=8)
        ax.set_xticks([])
        ax.grid(axis="y", linestyle="--", alpha=0.3)
        ax.legend(loc='center left', bbox_to_anchor=(1.02, 0.5), borderaxespad=0, frameon=False, fontsize=6)

        # Guardar directamente en el folder de outputs (NO en Resumen)
        out_dir = os.path.join(output_root, contract_code)
        os.makedirs(out_dir, exist_ok=True)
        out_png = os.path.join(out_dir, f"G3_Crecimiento_Scatter_{contract_code}.png")
        _print_size_cm(fig)
        fig.savefig(out_png, dpi=EXPORT_DPI, facecolor=None)
        plt.close(fig)
        print(f"🌱 Scatter plot de crecimiento (DBH) guardado: {out_png}")
        return out_png

    # ====================
    #     BAR CHART
    # ====================
    # Agrupar por plot y media de DBH
    grp = (
        df.groupby("plot")["dbh"]
        .mean()
        .reset_index(name="dbh_mean")
        .sort_values("dbh_mean")
    )
    plots = grp["plot"].astype(str).tolist()
    x = np.arange(len(plots))

    # Tus datos de inventario SIEMPRE están en pulgadas (DBH in)
    # Así que solo conviertes si quieres mostrar en cm
    if lang == "es":
        grp["dbh_mean"] = grp["dbh_mean"] * 2.54  # convierte a cm solo para graficar
    # Si lang == "en", dejas grp["dbh_mean"] igual (pulgadas)

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
    ax.set_ylabel(ylabel, fontsize=9)
    ax.set_xticks(x)
    ax.set_xticklabels('')
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    ax.legend(loc='center left', bbox_to_anchor=(1.02, 0.5), borderaxespad=0, frameon=False, fontsize=6)

    # Guardar en folder principal (NO en Resumen)
    out_dir = os.path.join(output_root, contract_code)
    os.makedirs(out_dir, exist_ok=True)
    out_png = os.path.join(out_dir, f"G3_Crecimiento_{contract_code}.png")
    _print_size_cm(fig)
    fig.savefig(out_png, dpi=EXPORT_DPI, facecolor=None)
    plt.close(fig)

    print(f"🌱 Gráfico de crecimiento (DBH) guardado: {out_png}")
    return out_png
