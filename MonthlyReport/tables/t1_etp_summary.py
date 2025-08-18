# MonthlyReport/tables/t1_etp_summary.py
from core.libs import pd
from MonthlyReport.tables_process import get_allocation_type

def build_etp_summary(engine):
    # 1) Base contratos + status
    cti = pd.read_sql(
        """
        SELECT 
            cti.contract_code, 
            cti.etp_year,
            cti.trees_contract, 
            cfi.status, 
            cti.region
        FROM masterdatabase.contract_tree_information AS cti
        LEFT JOIN masterdatabase.contract_farmer_information AS cfi
          ON cti.contract_code = cfi.contract_code
        """,
        engine
    )
    if cti.empty:
        return pd.DataFrame(columns=["Allocation Type", "region", "etp_year", "Total", "Survival"])

    cti["region"] = cti["region"].astype("string").str.strip()
    cti["etp_year"] = pd.to_numeric(cti["etp_year"], errors="coerce").astype("Int64")

    # 2) Totales por región/año
    total = cti.groupby(["region", "etp_year"])["contract_code"].count().rename("Total")

    # 3) Pivot de status
    pivot = pd.pivot_table(
        cti,
        index=["region", "etp_year"],
        columns="status",
        values="contract_code",
        aggfunc="count",
        fill_value=0
    )

    # 4) Une totales + pivot
    summary = pd.concat([total, pivot], axis=1)

    # 5) Survival (solo Active)
    survival_df = pd.read_sql(
        "SELECT contract_code, current_survival_pct, current_surviving_trees FROM masterdatabase.survival_current",
        engine
    )
    active_cti = cti[cti["status"] == "Active"]
    merged = active_cti.merge(survival_df, on="contract_code", how="left")

    def weighted_survival_pct(g):
        total_trees = g["trees_contract"].sum()
        surviving_trees = g["current_surviving_trees"].sum()
        if pd.notna(total_trees) and total_trees > 0:
            return round(100 * surviving_trees / total_trees, 1)
        return None

    region_survival = (
        merged.groupby(["region", "etp_year"])
              .apply(weighted_survival_pct)
              .apply(lambda x: f"{x}%" if x is not None else "N/A")
    )

    # 6) A plano + Survival
    summary = summary.reset_index()
    summary["Survival"] = summary.set_index(["region", "etp_year"]).index.map(region_survival).fillna("N/A")

    # 7) Asegura columnas de status que falten
    status_cols = list(pivot.columns)
    for col in status_cols:
        if col not in summary.columns:
            summary[col] = 0

    # 8) Allocation Type (primer campo) y orden
    def alloc_label(y):
        return "/".join(get_allocation_type(int(y))) if pd.notna(y) else pd.NA

    summary["Allocation Type"] = summary["etp_year"].apply(alloc_label).astype("string")

    # orden categórico de Allocation Type para sort estable
    alloc_order = ["COP", "COP/ETP", "ETP"]
    summary["Allocation Type"] = pd.Categorical(summary["Allocation Type"], categories=alloc_order, ordered=True)

    # 9) Orden de columnas
    dynamic_status = [c for c in status_cols if c in summary.columns]
    cols = ["Allocation Type", "region", "etp_year", "Total"] + dynamic_status + ["Survival"]
    summary = summary[cols]

    # 10) Orden de filas (y SIN 'index' basura: no uses reset_index otra vez)
    summary = summary.sort_values(by=["etp_year", "Allocation Type", "region"], ignore_index=True)

    return summary
