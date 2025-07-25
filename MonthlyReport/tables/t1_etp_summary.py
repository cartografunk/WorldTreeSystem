from core.libs import pd
from core.db import get_engine

def build_etp_summary(engine):

    # 1. Extrae todos los contratos y status
    cti = pd.read_sql(
        """
        SELECT 
            cti.contract_code, 
            cti.etp_year, 
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

    # 5. Survival: join con metrics (sin filtrar year)
    metrics = pd.read_sql(
        """
        SELECT contract_code, total_trees, survival
        FROM masterdatabase.inventory_metrics
        """,
        engine
    )
    merged = cti.merge(metrics, on="contract_code", how="left")

    if not merged.empty and merged["survival"].notna().any():
        merged["survival"] = pd.to_numeric(merged["survival"].str.replace('%', ''), errors='coerce') / 100
        merged["survivors"] = merged["total_trees"] * merged["survival"]

        def survival_pct(g):
            trees = g["total_trees"].sum()
            survivors = g["survivors"].sum()
            return 100 * survivors / trees if trees > 0 else None

        region_survival = (
            merged.groupby(["region", "etp_year"])
            .apply(survival_pct)
            .round(0)
            .astype("Int64")
            .astype(str) + "%"
        )
        summary["Survival"] = region_survival
        summary["Survival"] = summary["Survival"].replace('<NA>%', 'N/A').fillna('N/A')
    else:
        summary["Survival"] = 'N/A'

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

