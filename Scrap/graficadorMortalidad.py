import matplotlib.pyplot as plt
import pandas as pd
from sqlalchemy import create_engine
from ReportGenerator.utils.colors import COLOR_PALETTE
import os
from crecimientoesperadoCR import get_expected_diameter_growth

df_referencia = get_expected_diameter_growth()


# Conexión a la base de datos
engine = create_engine("postgresql+psycopg2://postgres:pauwlonia@localhost:5432/gisdb")

# Extraer contratos
contracts_df = pd.read_sql(
    'SELECT DISTINCT "id_contract" FROM public.cat_cr_inventory2025 ORDER BY "id_contract"',
    engine
)

# Iterar sobre cada contrato
for contract_code in contracts_df["id_contract"]:

    # === 1. Carpetas ===
    plots_dir = os.path.join("..", contract_code, "Mortalidad")
    altura_dir = os.path.join("..", contract_code, "Altura")
    resumen_dir = os.path.join("..", contract_code, "Resumen")

    os.makedirs(plots_dir, exist_ok=True)
    os.makedirs(altura_dir, exist_ok=True)
    os.makedirs(resumen_dir, exist_ok=True)

    # === 2. Mortalidad ===
    query_mortalidad = f"""
    SELECT
        "Plot#",
        SUM("alive_tree") AS "Vivos",
        SUM("dead_tree") AS "Muertos"
    FROM public.cr_inventory_2025
    WHERE "Contract Code" = '{contract_code}'
    GROUP BY "Plot#"
    ORDER BY "Plot#"
    """
    df_mort = pd.read_sql(query_mortalidad, engine)

    if df_mort.empty:
        print(f"⚠️ No hay datos de mortalidad para contrato {contract_code}.")
    else:
        vivos_total = 0
        muertos_total = 0

        for idx, row in df_mort.iterrows():
            plot = row["Plot#"]
            vivos = row["Vivos"]
            muertos = row["Muertos"]

            vivos_total += vivos
            muertos_total += muertos

            if vivos + muertos == 0:
                continue

            labels = ['Árboles Vivos', 'Árboles Muertos']
            sizes = [vivos, muertos]
            colores = [COLOR_PALETTE['secondary_green'], COLOR_PALETTE['primary_blue']]

            plt.figure(figsize=(6, 6))
            plt.pie(
                sizes, labels=labels, autopct='%1.1f%%',
                colors=colores, startangle=90,
                textprops={'fontsize': 12, 'color': COLOR_PALETTE['text_dark_gray']}
            )
            plt.title(f'Mortality - Parcela {plot}',
                      fontsize=14, color=COLOR_PALETTE['primary_blue'])

            output_file = os.path.join(plots_dir, f"{contract_code}_Mortality_P{int(plot):02d}.png")
            if not os.path.exists(output_file):
                plt.savefig(output_file, dpi=300, bbox_inches='tight', facecolor=COLOR_PALETTE['neutral_gray'])
                print(f"✅ Gráfico por parcela guardado: {output_file}")
            else:
                print(f"⚠️ Ya existe y no se sobreescribió: {output_file}")
            plt.close()

            print(f"✅ Gráfico por parcela guardado: {output_file}")

        if vivos_total + muertos_total > 0:
            resumen_labels = ['Árboles Vivos', 'Árboles Muertos']
            resumen_sizes = [vivos_total, muertos_total]
            resumen_colores = [COLOR_PALETTE['secondary_green'], COLOR_PALETTE['primary_blue']]

            plt.figure(figsize=(7, 7))
            plt.pie(
                resumen_sizes, labels=resumen_labels, autopct='%1.1f%%',
                colors=resumen_colores, startangle=90,
                textprops={'fontsize': 13, 'color': COLOR_PALETTE['text_dark_gray']}
            )
            plt.title(f'Mortality - {contract_code}',
                      fontsize=16, color=COLOR_PALETTE['primary_blue'])

            resumen_file = os.path.join(resumen_dir, f"G1_Mortality_{contract_code}.png")
            if not os.path.exists(resumen_file):
                plt.savefig(resumen_file, dpi=300, bbox_inches='tight', facecolor='white')
                print(f"📊 Gráfico resumen guardado: {resumen_file}")
            else:
                print(f"⚠️ Ya existe y no se sobreescribió: {resumen_file}")
            plt.close()


    # === 3. Altura ===
    query_altura = f"""
    SELECT "Plot#", "THT (ft)", "Merch. HT (ft)"
    FROM public.cr_inventory_2025
    WHERE "Contract Code" = '{contract_code}'
    """
    df_alt = pd.read_sql(query_altura, engine)

    if df_alt.empty:
        print(f"⚠️ No hay datos de altura para contrato {contract_code}.")
        continue

    df_grouped = df_alt.groupby("Plot#", dropna=True).agg({
        "THT (ft)": "mean",
        "Merch. HT (ft)": "mean"
    }).dropna().reset_index()

    if df_grouped.empty:
        print(f"⚠️ No hay datos válidos de altura en {contract_code}.")
        continue

    plots = df_grouped["Plot#"].astype(int).astype(str)
    altura_total = df_grouped["THT (ft)"]
    altura_comercial = df_grouped["Merch. HT (ft)"]

    bar_width = 0.35
    x = range(len(plots))

    fig, ax = plt.subplots(figsize=(10, 6))

    ax.bar(
        [p - bar_width/2 for p in x],
        altura_total,
        width=bar_width,
        label='Altura Total',
        color=COLOR_PALETTE['primary_blue']
    )

    ax.bar(
        [p + bar_width/2 for p in x],
        altura_comercial,
        width=bar_width,
        label='Altura Comercial',
        color=COLOR_PALETTE['accent_yellow']
    )

    ax.set_xticks(x)
    ax.set_xticklabels(plots)
    ax.set_ylabel("Altura promedio (ft)", fontsize=12)
    ax.set_xlabel("Parcela", fontsize=12)
    ax.set_title(f"Distribución de Altura - P{contract_code}", fontsize=14, color=COLOR_PALETTE['primary_blue'])
    ax.legend()

    altura_file = os.path.join(resumen_dir, f"G2_Altura_{contract_code}.png")
    if not os.path.exists(altura_file):
        plt.savefig(altura_file, dpi=300, bbox_inches='tight', facecolor=COLOR_PALETTE['neutral_gray'])
        print(f"📏 Gráfico de altura guardado: {altura_file}")
    else:
        print(f"⚠️ Ya existe y no se sobreescribió: {altura_file}")
    plt.close()

    # === 4. Gráficos por Plot de Altura ===
    for idx, row in df_grouped.iterrows():
        plot = row["Plot#"]
        total = row["THT (ft)"]
        comercial = row["Merch. HT (ft)"]

        fig, ax = plt.subplots(figsize=(5, 5))
        ax.bar(
            ['Altura Total', 'Altura Comercial'],
            [total, comercial],
            color=[COLOR_PALETTE['primary_blue'], COLOR_PALETTE['accent_yellow']]
        )
        ax.set_ylim(0, max(total, comercial) + 2)
        ax.set_ylabel("Altura (ft)")
        ax.set_title(f'Altura - P{int(plot):02d}', fontsize=12, color=COLOR_PALETTE['primary_blue'])

        plot_file = os.path.join(altura_dir, f"{contract_code}_Altura_P{int(plot):02d}.png")

        plt.tight_layout()  # Acomoda bien los elementos visuales del gráfico

        if not os.path.exists(plot_file):
            plt.savefig(plot_file, dpi=300, bbox_inches='tight', facecolor='white')
            print(f"📈 Gráfico de altura por parcela guardado: {plot_file}")
        else:
            print(f"⚠️ Ya existe y no se sobreescribió: {plot_file}")

        plt.close()

    # === 5. Comparativa de Crecimiento en Diámetro ===
    from datetime import datetime

    # Tabla de referencia esperada
    df_referencia = pd.DataFrame({
        "Año": [1, 2, 3, 4],
        "Min": [6, 8, 10, 13],
        "Ideal": [10, 9.5, 13, 16],
        "Max": [10, 11, 16, 19]
    })

    # Año actual
    año_actual = datetime.now().year

    # Obtener edad por contrato
    query_siembra = """
    SELECT DISTINCT "id_contract", "Año de Siembra"
    FROM public.cat_cr_inventory2025
    WHERE "Año de Siembra" IS NOT NULL
    """
    df_siembra = pd.read_sql(query_siembra, engine)
    df_siembra["Edad"] = año_actual - df_siembra["Año de Siembra"]

    # Obtener promedio de diámetro por contrato
    query_diametro = """
    SELECT 
        "Contract Code" AS id_contract,
        AVG("DBH (in)") * 2.54 AS promedio_cm  -- conversión a centímetros
    FROM public.cr_inventory_2025
    WHERE "DBH (in)" IS NOT NULL
    GROUP BY "Contract Code"
    """
    df_diametro = pd.read_sql(query_diametro, engine)

    # Unir datos
    df_final = pd.merge(df_siembra, df_diametro, on="id_contract", how="inner")

    # Crear gráfica
    fig, ax = plt.subplots(figsize=(10, 6))

    # Líneas esperadas
    ax.plot(df_referencia["Año"], df_referencia["Min"], label="Mínimo Esperado", linestyle="--", color="gray")
    ax.plot(df_referencia["Año"], df_referencia["Max"], label="Máximo Esperado", linestyle="--", color="gray")
    ax.plot(df_referencia["Año"], df_referencia["Ideal"], label="Ideal", color=COLOR_PALETTE['accent_yellow'])

    # Área sombreada entre min y max
    ax.fill_between(df_referencia["Año"], df_referencia["Min"], df_referencia["Max"],
                    color=COLOR_PALETTE['neutral_gray'], alpha=0.2)

    # Datos reales
    for idx, row in df_final.iterrows():
        ax.scatter(row["Edad"], row["promedio_cm"], label=row["id_contract"], color=COLOR_PALETTE['primary_blue'])

    ax.set_xlabel("Edad de la plantación (años)", fontsize=12)
    ax.set_ylabel("Diámetro promedio (cm)", fontsize=12)
    ax.set_title("G3 - Comparativa de Crecimiento en Diámetro", fontsize=14, color=COLOR_PALETTE['primary_blue'])
    ax.grid(True)
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')

    # Guardar gráfico
    output_file = "G3_Comparativa_Diametro.png"
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()

    print(f"📊 Gráfico de comparativa de diámetro guardado: {output_file}")
