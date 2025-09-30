# MonthlyReport/tables/t2_trees_by_etp_raise.py

from core.libs import pd
from MonthlyReport.utils_monthly_base import build_monthly_base_table

DISPLAY_MAP  = {"CR": "Costa Rica", "GT": "Guatemala", "MX": "Mexico", "US": "USA"}
DISPLAY_COLS = ["Costa Rica", "Guatemala", "Mexico", "USA"]

def build_etp_trees_table2(engine):
    mbt = build_monthly_base_table()
    if mbt.empty:
        return pd.DataFrame(columns=["year","etp","contract_trees_status"] + DISPLAY_COLS + ["Total"])

    # Solo contratos con componente ETP (ETP puros o mixtos)
    mbt = mbt[mbt["etp_type"].isin(["ETP","ETP/COP"])].copy()
    mbt["etp"] = "ETP"

    # Tipos numéricos seguros
    for c in ["contracted_etp","planted_etp","current_surviving_trees"]:
        if c in mbt.columns:
            mbt[c] = pd.to_numeric(mbt[c], errors="coerce").fillna(0)

    # Agregados por año/región
    g = (
        mbt.groupby(["etp_year","region"], dropna=False)
           .agg(
               Contracted=("contracted_etp","sum"),
               Planted   =("planted_etp","sum"),
               Surviving =("current_surviving_trees","sum")
           )
           .reset_index()
    )

    # Long → pivot
    df_long = g.melt(
        id_vars=["etp_year","region"],
        value_vars=["Contracted","Planted","Surviving"],
        var_name="contract_trees_status",
        value_name="value"
    )

    totals_fallback = (
        df_long.groupby(["etp_year","contract_trees_status"], dropna=False)["value"]
               .sum(min_count=1)
               .reset_index()
               .rename(columns={"value":"Total_fallback"})
    )

    piv = df_long.pivot_table(
        index=["etp_year","contract_trees_status"],
        columns="region",
        values="value",
        aggfunc="sum",
        fill_value=0
    ).reset_index()

    # Países visibles
    for pref, disp in DISPLAY_MAP.items():
        piv[disp] = pd.to_numeric(piv.get(pref, 0), errors="coerce").fillna(0)

    piv["Total"] = piv[DISPLAY_COLS].sum(axis=1)
    piv["year"]  = pd.to_numeric(piv["etp_year"], errors="coerce").astype(int)
    piv["etp"]   = "ETP"

    out = piv[["year","etp","contract_trees_status"] + DISPLAY_COLS + ["Total"]].copy()

    # Fallback de Total
    out = out.merge(
        totals_fallback.rename(columns={"etp_year":"year"}),
        on=["year","contract_trees_status"],
        how="left"
    )
    out["Total"] = pd.to_numeric(out["Total_fallback"], errors="coerce").fillna(out["Total"]).astype(int)

    # Tipos y orden final
    out["contract_trees_status"] = pd.Categorical(
        out["contract_trees_status"],
        categories=["Contracted","Planted","Surviving"],
        ordered=True
    )
    for c in DISPLAY_COLS + ["Total"]:
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0).astype(int)

    out = out.sort_values(by=["year","contract_trees_status"], ignore_index=True)
    out = out.drop(columns=["Total_fallback"], errors="ignore")
    return out
