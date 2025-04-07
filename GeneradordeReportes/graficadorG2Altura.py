from utils.libs import pd, os, plt, rcParams
from utils.db import get_engine
from utils.colors import COLOR_PALETTE
from utils.plot import save_bar_chart


def generar_altura(contract_code: str, output_root: str = "outputs"):
    engine = get_engine()

    # === 1. Carpetas ===
    altura_dir = os.path.join(output_root, contract_code, "Altura")
    resumen_dir = os.path.join(output_root, contract_code, "Resumen")
    os.makedirs(altura_dir, exist_ok=True)
    os.makedirs(resumen_dir, exist_ok=True)

    # === 2. Consulta a la base de datos ===
    query = f"""
    SELECT "Plot#", "THT (ft)", "Merch. HT (ft)"
    FROM public.cr_inventory_2025
    WHERE "Contract Code" = '{contract_code}'
    """
    df_alt = pd.read_sql(query, engine)

    if df_alt.empty:
        print(f"⚠️ No hay datos de altura para contrato {contract_code}.")
        return

    df_grouped = df_alt.groupby("Plot#", dropna=True).agg({
        "THT (ft)": "mean",
        "Merch. HT (ft)": "mean"
    }).dropna().reset_index()

    if df_grouped.empty:
        print(f"⚠️ No hay datos válidos de altura en {contract_code}.")
        return

    # === 3. Gráfico resumen de altura por parcela (G2) ===
    plots = df_grouped["Plot#"].astype(int).astype(str).tolist()
    altura_total = df_grouped["THT (ft)"].tolist()
    altura_comercial = df_grouped["Merch. HT (ft)"].tolist()

    series = {
        "Altura Total": altura_total,
        "Altura Comercial": altura_comercial
    }

    altura_file = os.path.join(output_root, resumen_dir, f"G2_Altura_{contract_code}.png")
    save_bar_chart(
        x_labels=plots,
        series=series,
        title=f"Distribución de Altura - P{contract_code}",
        output_path=altura_file,
        ylabel="Altura promedio (ft)",
        xlabel="Parcela",
        colors=[COLOR_PALETTE['primary_blue'], COLOR_PALETTE['accent_yellow']]
    )

    # === 4. Gráficos individuales por parcela ===
    for _, row in df_grouped.iterrows():
        plot = int(row["Plot#"])
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
        ax.set_title(f'Altura - P{plot:02d}', fontsize=12, color=COLOR_PALETTE['primary_blue'])

        plot_file = os.path.join(altura_dir, f"{contract_code}_Altura_P{plot:02d}.png")

        if not os.path.exists(plot_file):
            plt.tight_layout()
            plt.savefig(plot_file, dpi=300, bbox_inches='tight', facecolor='white')
            print(f"📈 Parcela {plot} guardada: {plot_file}")
        else:
            print(f"⚠️ Ya existe y no se sobreescribió: {plot_file}")
        plt.close()


# === Ejecución directa ===
if __name__ == "__main__":
    engine = get_engine()
    contracts_df = pd.read_sql(
        'SELECT DISTINCT "id_contract" FROM public.cat_cr_inventory2025 ORDER BY "id_contract"',
        engine
    )
    for code in contracts_df["id_contract"]:
        generar_altura(code)
