from core.libs import pd
from core.db import get_engine

def build_etp_summary(engine):

    # 1. Extrae todos los contratos y status
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
        print(f"⚠️  No hay contratos en la base")
        return pd.DataFrame(columns=["region", "etp_year", "Total", "Survival"])

    # 2. Total contratos por región y etp_year
    total = cti.groupby(["region", "etp_year"])["contract_code"].count().rename("Total")

    # 3. Pivot de status
    pivot = pd.pivot_table(
        cti,
        index=["region", "etp_year"],
        columns="status",
        values="contract_code",
        aggfunc="count",
        fill_value=0
    )

    # 4. Une totales y pivot
    summary = pd.concat([total, pivot], axis=1)

    # 5. Survival: usar survival_current y solo contratos Active
    survival_df = pd.read_sql(
        """
        SELECT contract_code, current_survival_pct, current_surviving_trees
        FROM masterdatabase.survival_current
        """,
        engine
    )

    # Filtramos contratos activos y añadimos survival
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

    # Mapear survival calculado al summary
    summary = summary.reset_index()
    summary["Survival"] = summary.set_index(["region", "etp_year"]).index.map(region_survival).fillna("N/A")

    # 6. Rellena con 0s si faltan status
    status_cols = list(pivot.columns)
    for col in status_cols:
        if col not in summary.columns:
            summary[col] = 0

    # 7. Ordena columnas
    summary = summary.reset_index()
    dynamic_status = [c for c in summary.columns if c not in ["region", "etp_year", "Total", "Survival"]]
    cols = ["region", "etp_year", "Total"] + dynamic_status + ["Survival"]
    summary = summary[cols]

    # 8. (Opcional) Puedes agregar totales por año, región, o ambos aquí si quieres

    return summary

if __name__ == "__main__":
    engine = get_engine()
    df = build_etp_summary(engine)
    print(df)
    df.to_excel("etp_summary_ALL.xlsx", index=False)

