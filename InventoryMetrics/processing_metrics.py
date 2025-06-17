#InventoryMetrics/processing_metrics

from core.schema_helpers import get_column

def aggregate_contracts(
    df,
    country=None,
    year=None,
    include_all_contracts=False,
    filter_valid=True,
    required_fields=None,
    extra_cols=None,
    include_progress=True
):
    """
    Agrupa por contrato y calcula métricas. Opcionalmente:
    - filtra sólo los registros con datos válidos
    - agrega contratos aunque no tengan datos válidos
    - permite campos extra o calcular sólo un subconjunto

    Args:
        df: DataFrame origen.
        country, year: se asignan si se requiere.
        include_all_contracts: mergea todos los contracts aunque estén vacíos.
        filter_valid: filtra por campos obligatorios no nulos (default True).
        required_fields: lista de campos clave (por defecto DBH y THT y DOYLE).
        extra_cols: métricas extra a calcular.
        include_progress: agrega el campo "progress".

    Returns:
        DataFrame agrupado y con métricas por contrato.
    """
    if required_fields is None:
        required_fields = ["dbh_in", "tht_ft", "doyle_bf"]
    contract_col = get_column("contractcode", df)

    # Filtra registros válidos si aplica
    if filter_valid:
        for field in required_fields:
            df = df[df[get_column(field, df)].notna()]

    grouped = df.groupby(contract_col)
    rows = []
    for contract_code, group in grouped:
        row = {
            "contract_code": contract_code,
            "total_trees": group.shape[0]
        }
        if country:
            row["country_code"] = country
        if year:
            row["year"] = year

        # Campos críticos
        for field in required_fields:
            col = get_column(field, group)
            row[f"{field}_mean"] = pd.to_numeric(group[col], errors='coerce').mean()

        # Doyle total si está entre los requeridos
        if "doyle_bf" in required_fields:
            doyle_col = get_column("doyle_bf", group)
            row["doyle_total"] = pd.to_numeric(group[doyle_col], errors='coerce').sum()

        # Extras opcionales
        if extra_cols:
            for col in extra_cols:
                row[col] = group[get_column(col, group)].mean()

        rows.append(row)

    df_metrics = pd.DataFrame(rows)

    # Mergea todos los contratos si se pide
    if include_all_contracts:
        contracts = df[contract_col].dropna().unique()
        df_contracts = pd.DataFrame({"contract_code": contracts})
        df_metrics = df_contracts.merge(df_metrics, on="contract_code", how="left")

    # Agrega campo "progress"
    if include_progress:
        def status(row):
            crit_ok = all(pd.notnull(row.get(f"{f}_mean")) for f in required_fields)
            return "OK" if crit_ok else "error"
        df_metrics["progress"] = df_metrics.apply(status, axis=1)

    return df_metrics
