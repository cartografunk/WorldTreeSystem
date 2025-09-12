# MonthlyReport/tables/t2_trees_by_etp_raise.py

from core.libs import pd, np
from MonthlyReport.utils_monthly_base import build_monthly_base_table
from MonthlyReport.tables_process import get_allocation_type

DISPLAY_MAP  = {"CR": "Costa Rica", "GT": "Guatemala", "MX": "Mexico", "US": "USA"}
DISPLAY_COLS = ["Costa Rica", "Guatemala", "Mexico", "USA"]

def build_etp_trees_table2(engine):
    mbt = build_monthly_base_table()
    if mbt.empty:
        return pd.DataFrame(columns=["year","etp","contract_trees_status"] + DISPLAY_COLS + ["Total"])

    # Tipos (no tocamos 'region')
    mbt["etp_year"]       = pd.to_numeric(mbt.get("etp_year"), errors="coerce").astype("Int64")
    mbt["trees_contract"] = pd.to_numeric(mbt.get("trees_contract"), errors="coerce").fillna(0)
    mbt["planted"]        = pd.to_numeric(mbt.get("planted"), errors="coerce").fillna(0)
    mbt["alive_sc"]       = pd.to_numeric(mbt.get("alive_sc"), errors="coerce").fillna(0)
    mbt["contracted_cop"] = pd.to_numeric(mbt.get("contracted_cop"), errors="coerce").fillna(0)
    mbt["planted_cop"]    = pd.to_numeric(mbt.get("planted_cop"), errors="coerce").fillna(0)
    if "has_cop" not in mbt.columns:
        mbt["has_cop"] = False

    # Asignar etiqueta por cohorte, sin loops
    years = sorted(mbt["etp_year"].dropna().astype(int).unique().tolist())
    alloc_map = {y: "/".join(get_allocation_type(int(y))) for y in years}
    mbt["allocation_type"] = mbt["etp_year"].map(alloc_map)

    # Columnas "de uso" vectorizadas
    mbt["Contracted_use"] = 0.0
    mbt["Planted_use"]    = 0.0

    cop = mbt["allocation_type"].eq("COP")
    etp = mbt["allocation_type"].eq("ETP")
    mix = mbt["allocation_type"].eq("COP/ETP")

    # COP: solo contratos con COP
    mbt.loc[cop & mbt["has_cop"], "Contracted_use"] = mbt.loc[cop & mbt["has_cop"], "contracted_cop"]
    mbt.loc[cop & mbt["has_cop"], "Planted_use"]    = mbt.loc[cop & mbt["has_cop"], "planted_cop"]

    # ETP: todos ETP
    mbt.loc[etp, "Contracted_use"] = mbt.loc[etp, "trees_contract"]
    mbt.loc[etp, "Planted_use"]    = mbt.loc[etp, "planted"]

    # COP/ETP: toma COP si tiene, si no ETP
    mbt.loc[mix, "Contracted_use"] = np.where(
        mbt.loc[mix, "has_cop"], mbt.loc[mix, "contracted_cop"], mbt.loc[mix, "trees_contract"]
    )
    mbt.loc[mix, "Planted_use"] = np.where(
        mbt.loc[mix, "has_cop"], mbt.loc[mix, "planted_cop"], mbt.loc[mix, "planted"]
    )

    # Agregado por cohorte/region (una sola pasada)
    g = (
        mbt.groupby(["etp_year","allocation_type","region"], dropna=False)
           .agg(Contracted=("Contracted_use","sum"),
                Planted=("Planted_use","sum"),
                Surviving=("alive_sc","sum"))
           .reset_index()
    )

    # Integridad post-agrupación
    g["Surviving"] = g["Surviving"].clip(lower=0)
    g["Surviving"] = g[["Surviving","Contracted"]].min(axis=1)
    g["Surviving"] = g[["Surviving","Planted"]].min(axis=1)

    # Long → pivot por región
    df_long = g.melt(
        id_vars=["etp_year","allocation_type","region"],
        value_vars=["Contracted","Planted","Surviving"],
        var_name="contract_trees_status",
        value_name="value"
    )

    totals_fallback = (
        df_long.groupby(["etp_year","allocation_type","contract_trees_status"], dropna=False)["value"]
               .sum(min_count=1)
               .reset_index()
               .rename(columns={"value":"Total_fallback"})
    )

    piv = df_long.pivot_table(
        index=["etp_year","allocation_type","contract_trees_status"],
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
    piv["etp"]   = piv["allocation_type"].astype("string")

    out = piv[["year","etp","contract_trees_status"] + DISPLAY_COLS + ["Total"]].copy()

    # Fallback de Total (cohorte/status)
    out = out.merge(
        totals_fallback.rename(columns={"etp_year":"year","allocation_type":"etp"}),
        on=["year","etp","contract_trees_status"],
        how="left"
    )
    out["Total"] = pd.to_numeric(out["Total_fallback"], errors="coerce").fillna(0).astype(int)

    # Tipos y orden final
    out["contract_trees_status"] = pd.Categorical(
        out["contract_trees_status"], categories=["Contracted","Planted","Surviving"], ordered=True
    )
    for c in DISPLAY_COLS + ["Total"]:
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0).astype(int)

    out = out.sort_values(by=["year","etp","contract_trees_status"], ignore_index=True)
    out = out.drop(columns=["Total_fallback"], errors="ignore")
    return out
