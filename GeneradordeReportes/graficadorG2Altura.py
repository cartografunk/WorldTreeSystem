from GeneradordeReportes.utils.libs import plt, rcParams, os, pd
from GeneradordeReportes.utils.db import get_engine
from GeneradordeReportes.utils.colors import COLOR_PALETTE
from GeneradordeReportes.utils.config import BASE_DIR, EXPORT_DPI
from GeneradordeReportes.utils.plot import FIGSIZE, _print_size_cm
from GeneradordeReportes.utils.helpers import (
    get_inventory_table_name,
    get_sql_column,
    get_region_language,
    resolve_column
)
from GeneradordeReportes.utils.text_templates import text_templates
from GeneradordeReportes.utils.crecimiento_esperado import df_altura  # solo Min y Max


def generar_altura(contract_code: str, country: str, year: int, engine=None, output_root: str = os.path.join(BASE_DIR, "GeneradordeReportes", "outputs") ):
    engine = engine or get_engine()
    inv_table = get_inventory_table_name(country, year)  # p.ej. inventory_cr_2025

    # 1) Resolve columnas en inventario
    plot_col      = resolve_column(engine, inv_table, "plot")
    tht_col       = resolve_column(engine, inv_table, "tht_ft")
    # esto devolverá exactamente 'Contract Code'
    inv_code_col  = resolve_column(engine, inv_table, "contractcode")

    # 2) Query de alturas de inventario
    sql_heights = f"""
    SELECT
      "{plot_col}" AS plot,
      "{tht_col}"  AS tht
    FROM public.{inv_table}
    WHERE "{inv_code_col}" = %(code)s
    """
    df = pd.read_sql(sql_heights, engine, params={"code": contract_code})
    if df.empty:
        print(f"⚠️ Sin datos de altura para {contract_code}.")
        return

    # 3) Resolve columna de contrato y planting_year en la tabla principal
    tree_table    = "masterdatabase.contract_tree_information"
    # aquí key='contractcode' también, pero en esta tabla su sql_name real es contract_code
    tree_code_col = resolve_column(engine, tree_table,   "contractcode")
    plant_col     = resolve_column(engine, tree_table,   "planting_year")

    # 4) Query de planting_year
    sql_plant = f"""
    SELECT "{plant_col}" AS planting_year
    FROM {tree_table}
    WHERE "{tree_code_col}" = %(code)s
    """
    plant_df = pd.read_sql(sql_plant, engine, params={"code": contract_code})
    if plant_df.empty or plant_df.iloc[0,0] is None:
        print(f"⚠️ No se encontró planting_year para {contract_code}.")
        return
    planting_year = int(plant_df.iloc[0,0])

    # 5) Calcular edad y seguir con el gráfico…
    df["age"] = year - planting_year
    df = df.dropna(subset=["tht","age"])
    if df.empty:
        print(f"⚠️ Tras limpieza, no quedan datos válidos para contrato {contract_code}.")
        return

    # Preparar texto y paths
    lang    = get_region_language(country)
    title   = text_templates["chart_titles"]["height"][lang].format(code=contract_code)
    xlabel  = text_templates["chart_axes"]["height_x"][lang]
    ylabel  = text_templates["chart_axes"]["height_y"][lang]
    out_png = os.path.join(resumen_dir, f"G2_Altura_{contract_code}.png")

    if os.path.exists(out_png):
        print(f"⚠️ Ya existe: {out_png}")
        return out_png

    # Plot
    rcParams.update({'figure.autolayout': True})
    fig, ax = plt.subplots(figsize=FIGSIZE)

    # Scatter de alturas reales
    ax.scatter(
        df['age'], df['tht'],
        label="Mediciones reales",
        alpha=0.6, s=30,
        color=COLOR_PALETTE['primary_blue']
    )

    # Líneas de crecimiento Min y Max
    ages = df_altura['Año']
    ax.plot(
        ages, df_altura['Min'], linestyle='--',
        label="Mínimo esperado",
        color=COLOR_PALETTE['accent_yellow']
    )
    ax.plot(
        ages, df_altura['Max'], linestyle=':',
        label="Máximo esperado",
        color=COLOR_PALETTE['secondary_green']
    )

    # Labels, legend and grid
    ax.set_title(title, fontsize=11, color=COLOR_PALETTE['primary_blue'])
    ax.set_xlabel(xlabel, fontsize=9)
    ax.set_ylabel(ylabel, fontsize=9)
    ax.legend()
    ax.grid(True, linestyle='--', alpha=0.4)

    # Guardar
    _print_size_cm(fig)
    fig.savefig(out_png, dpi=EXPORT_DPI, facecolor=None)
    plt.close(fig)

    print(f"📊 Scatter de altura guardado: {out_png}")
    return out_png
