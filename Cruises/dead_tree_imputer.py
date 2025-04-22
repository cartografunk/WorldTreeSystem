from utils.libs import np, pd

def add_imputed_dead_rows(df: pd.DataFrame, contract_col: str, plot_col: str, dead_col: str) -> pd.DataFrame:
    """
    Para cada parcela con exactamente un árbol muerto (dead_col == 1), duplica esa fila
    hasta alcanzar el promedio de árboles por parcela (redondeado hacia abajo) para ese contrato.

    Args:
        df: DataFrame incluyendo al menos contract_col, plot_col y dead_col.
        contract_col: Nombre de la columna de contrato.
        plot_col: Nombre de la columna de parcela.
        dead_col: Columna entera/boleana indicando árbol muerto (1) o vivo (0).

    Returns:
        DataFrame con filas adicionales imputadas para árboles muertos faltantes.
    """
    df = df.copy()
    # Contar total de árboles por parcela
    plot_counts = (
        df
        .groupby([contract_col, plot_col])
        .size()
        .reset_index(name='plot_size')
    )
    # Promedio de árboles por parcela por contrato (floored)
    avg_per_contract = (
        plot_counts
        .groupby(contract_col)['plot_size']
        .mean()
        .apply(np.floor)
        .astype(int)
        .to_dict()
    )
    # Filtrar filas con dead_col == 1
    dead_rows = df[df[dead_col] == 1]
    rows_to_add = []
    for (_, row) in dead_rows.iterrows():
        contract = row[contract_col]
        plot = row[plot_col]
        current_dead = ((dead_rows[contract_col] == contract) & (dead_rows[plot_col] == plot)).sum()
        needed = avg_per_contract.get(contract, 0) - current_dead
        for _ in range(max(0, needed)):
            rows_to_add.append(row.to_dict())
    if rows_to_add:
        df = pd.concat([df, pd.DataFrame(rows_to_add)], ignore_index=True)

    print("Imputado de dead rows")
    return df



