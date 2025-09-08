# MonthlyReport/tables/t1_etp_summary.py
from core.libs import pd
from MonthlyReport.tables_process import get_allocation_type
from core.region import region_from_code  # 👈 derivamos región del code

def build_etp_summary(engine):
    # Solo CTI (status y trees_contract están aquí)
    cti = pd.read_sql(
        """
        SELECT 
            contract_code, 
            etp_year,
            trees_contract,
            status
        FROM masterdatabase.contract_tree_information
        """,
        engine
    )
    if cti.empty:
        return pd.DataFrame(columns=["Allocation Type", "region", "etp_year", "Total", "Survival"])

    # Normaliza dtypes
    cti["etp_year"] = pd.to_numeric(cti["etp_year"], errors="coerce").astype("Int64")
    cti["trees_contract"] = pd.to_numeric(cti["trees_contract"], errors="coerce").fillna(0)

    # Región 100% desde contract_code (evita NA)
    cti["region"] = cti["contract_code"].map(region_from_code).astype("string")

    # Status normalizado y filtro Active
    status_norm = (
        cti["status"].astype("string")
                     .str.normalize("NFKD").str.encode("ascii","ignore").str.decode("ascii")
                     .str.strip().str.lower()
    )
    cti["status_norm"] = status_norm

    # 1) Totales por región/año (contratos únicos)
    total = (
        cti.groupby(["region", "etp_year"], dropna=False)["contract_code"]
           .nunique().rename("Total")
    )

    # 2) Pivot de status
    pivot = pd.pivot_table(
        cti.assign(status=status_norm),             # usa status normalizado
        index=["region", "etp_year"],
        columns="status",
        values="contract_code",
        aggfunc=pd.Series.nunique,
        fill_value=0,
    )
    pivot.columns.name = None

    # 3) Alinear índices y unir
    idx = total.index.union(pivot.index)
    summary = pd.concat([total.reindex(idx).to_frame(), pivot.reindex(idx).fillna(0)], axis=1).reset_index()

    # 4) Survival ponderado (solo Active)
    surv = pd.read_sql(
        "SELECT contract_code, current_surviving_trees FROM masterdatabase.survival_current",
        engine
    )
    active = (
        cti[cti["status_norm"] == "active"]
          .merge(surv, on="contract_code", how="left")
          .copy()
    )
    active["current_surviving_trees"] = pd.to_numeric(active["current_surviving_trees"], errors="coerce").fillna(0)

    def _weighted_survival(g):
        den = g["trees_contract"].sum()
        num = g["current_surviving_trees"].sum()
        return f"{round(100 * num / den, 1)}%" if den > 0 else "N/A"

    region_survival = (
        active.groupby(["region", "etp_year"], dropna=False)
              .apply(_weighted_survival)
    )
    summary["Survival"] = [region_survival.get((r, y), "N/A") for r, y in zip(summary["region"], summary["etp_year"])]

    # 5) Allocation Type y orden
    def alloc_label(y):
        return "/".join(get_allocation_type(int(y))) if pd.notna(y) else pd.NA
    summary["Allocation Type"] = summary["etp_year"].apply(alloc_label).astype("string")

    alloc_order = ["COP", "COP/ETP", "ETP"]
    summary["Allocation Type"] = pd.Categorical(summary["Allocation Type"], categories=alloc_order, ordered=True)

    # 6) Columnas dinámicas de status
    status_cols = [c for c in pivot.columns if c in summary.columns]
    cols = ["Allocation Type", "region", "etp_year", "Total"] + status_cols + ["Survival"]
    summary = summary[cols].sort_values(by=["etp_year", "Allocation Type", "region"], ignore_index=True)
    return summary
