from core.libs import np, pd


def add_imputed_dead_rows(df: pd.DataFrame, contract_col: str, plot_col: str, dead_col: str) -> pd.DataFrame:
    """
    Imputa árboles muertos SOLO en parcelas con:
    - 1 árbol muerto (dead_col == 1)
    - 1 árbol en total (usando tree_number para contar).
    """
    df = df.copy()

    # 1. Calcular estadísticas por parcela (usando tree_number)
    plot_stats = (
        df.groupby([contract_col, plot_col], as_index=False)
        .agg(
            total_arboles=('tree_number', 'nunique'),
            muertos_parcela=(dead_col, 'sum')
        )
    )

    # 2. Calcular estadísticas por parcela (usando tree_number)
    valid_plots = plot_stats[
        (plot_stats['muertos_parcela'] == 1) &
        (plot_stats['total_arboles'] == 1)
        ]



    # 4. Calcular promedio por contrato (excluyendo parcelas inválidas)
    avg_per_contract = (
        plot_stats[plot_stats['total_arboles'] > 1]  # Ignorar parcelas de 1 árbol
        .groupby(contract_col)['total_arboles']
        .mean()
        .apply(np.floor)
        .astype(int)
        .to_dict()
    )

    # 5. Generar filas a imputar
    rows_to_add = []
    for _, row in valid_plots.iterrows():
        contract = row[contract_col]
        plot = row[plot_col]
        target_count = avg_per_contract.get(contract, 0)

        # Obtener fila original del árbol muerto
        dead_row = df[
            (df[contract_col] == contract) &
            (df[plot_col] == plot) &
            (df[dead_col] == 1)
            ].iloc[0]

        # Calcular imputaciones necesarias
        needed = max(0, target_count - 1)  # Restar el árbol existente

        for _ in range(needed):
            rows_to_add.append(dead_row.to_dict())

    # 6. Añadir filas y reportar
    if rows_to_add:
        added = pd.DataFrame(rows_to_add)
        df = pd.concat([df, added], ignore_index=True)
        print("\n=== 🪵 Resumen de imputación ===")
        print(f"Árboles imputados: {len(rows_to_add)}")
        print("\n📋 Por contrato:")
        print(added[contract_col].value_counts().sort_index())
    else:
        print("\n✅ No se imputaron árboles")

    return df