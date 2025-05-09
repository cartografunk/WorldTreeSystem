import os
import pandas as pd
from GeneradordeReportes.utils.libs import plt, rcParams
from GeneradordeReportes.utils.db import get_engine
from GeneradordeReportes.utils.colors import COLOR_PALETTE
from GeneradordeReportes.utils.config import BASE_DIR, EXPORT_DPI
from GeneradordeReportes.utils.plot import FIGSIZE, _print_size_cm
from GeneradordeReportes.utils.helpers import (
    get_inventory_table_name,
    get_sql_column,
    get_region_language
)
from GeneradordeReportes.utils.text_templates import text_templates
from GeneradordeReportes.utils.crecimiento_esperado import df_altura  # solo Min y Max


def generar_altura(contract_code: str,
                   country: str,
                   year: int,
                   engine=None,
                   output_root: str = os.path.join(BASE_DIR,
                                                   "GeneradordeReportes",
                                                   "outputs")):
    # Inicializar engine y directorio de salida
    engine = engine or get_engine()
    resumen_dir = os.path.join(output_root, contract_code, "Resumen")
    os.makedirs(resumen_dir, exist_ok=True)

    # Columnas dinámicas
    plot_col = get_sql_column("plot")  # p.ej. "Plot#"
    tht_col = get_sql_column("tht_ft")  # p.ej. "THT (ft)"
    contract_col = get_sql_column("contractcode")  # ya mapea a contractcode

    # Obtener datos de altura
    sql_heights = f"""
    SELECT
      "{plot_col}" AS plot,
      "{tht_col}"  AS tht
    FROM public.{table}
    WHERE {contract_col} = %(code)s
    """
    df = pd.read_sql(sql_heights, engine, params={"code": contract_code})
    if df.empty:
        print(f"⚠️ No hay datos de altura para contrato {contract_code}.")
        return

    # Obtener planting_year del registro maestro
    sql_plant = f"""
    SELECT planting_year
    FROM masterdatabase.contract_tree_information
    WHERE contract_code = :code
    """
    plant_df = pd.read_sql(sql_plant, engine, params={"code": contract_code})
    if plant_df.empty or pd.isna(plant_df.iloc[0,0]):
        print(f"⚠️ No se encontró planting_year para contrato {contract_code}.")
        return
    planting_year = int(plant_df.iloc[0,0])

    # Calcular edad y limpiar nulos
    df['age'] = year - planting_year
    df = df.dropna(subset=["tht", "age"])
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
