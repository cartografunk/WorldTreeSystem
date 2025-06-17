#InventoryMetrics/processing_metrics
from core.libs import pd, np
from core.schema_helpers import get_column
from core.db import get_engine
from InventoryMetrics.generate_helpers import add_cruise_date_to_metrics

engine = get_engine()

def aggregate_contracts(
    df,
    engine,
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
        # Calcula sobrevivencia/mortalidad
        alive_col = get_column("alive_tree", group)
        dead_col = get_column("dead_tree", group)
        total_alive = group[alive_col].sum() if alive_col in group else np.nan
        total_dead = group[dead_col].sum() if dead_col in group else np.nan
        total_trees = total_alive + total_dead if not np.isnan(total_alive) and not np.isnan(total_dead) else np.nan
        survival = round((total_alive / total_trees) * 100, 2) if total_trees else np.nan
        mortality = round((total_dead / total_trees) * 100, 2) if total_trees else np.nan

        # Redondeo y columnas igual que antes
        row = {
            "contract_code": contract_code,
            "inventory_year": year,
            "inventory_date": None,  # Se rellena después con el merge
            "total_trees": total_trees,
            "survival": f"{survival}%" if not np.isnan(survival) else None,
            "mortality": f"{mortality}%" if not np.isnan(mortality) else None,
            "dbh_mean": round(pd.to_numeric(group[get_column("dbh_in", group)], errors='coerce').mean(), 2),
            "dbh_std": round(pd.to_numeric(group[get_column("dbh_in", group)], errors='coerce').std(), 2),
            "tht_mean": round(pd.to_numeric(group[get_column("tht_ft", group)], errors='coerce').mean(), 2),
            "tht_std": round(pd.to_numeric(group[get_column("tht_ft", group)], errors='coerce').std(), 2),
            "mht_mean": round(pd.to_numeric(group[get_column("merch_ht_ft", group)], errors='coerce').mean(), 2),
            "mht_std": round(pd.to_numeric(group[get_column("merch_ht_ft", group)], errors='coerce').std(), 2),
            "doyle_bf_mean": round(pd.to_numeric(group[get_column("doyle_bf", group)], errors='coerce').mean(), 2),
            "doyle_bf_std": round(pd.to_numeric(group[get_column("doyle_bf", group)], errors='coerce').std(), 2),
            "doyle_bf_total": round(pd.to_numeric(group[get_column("doyle_bf", group)], errors='coerce').sum(), 2),
        }
        rows.append(row)

    df_metrics = pd.DataFrame(rows)
    df_metrics = add_cruise_date_to_metrics(engine, df_metrics, country, year)

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
