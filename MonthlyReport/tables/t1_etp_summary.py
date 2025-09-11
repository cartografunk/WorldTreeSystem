from core.libs import pd, np
from MonthlyReport.utils_monthly_base import build_monthly_base_table
from MonthlyReport.tables_process import get_allocation_type, fmt_pct_1d  # ← úsalo aquí

ACTIVE_STATUSES = ("Active",)  # ajusta a tus valores EXACTOS

def build_etp_summary(engine=None) -> pd.DataFrame:
    mbt = build_monthly_base_table()

    status_counts = (
        mbt.groupby(["allocation_type_str","region","etp_year","status"], dropna=False)["contract_code"]
           .nunique()
           .unstack("status", fill_value=0)
           .reset_index()
    )

    g_glb = mbt.groupby(["allocation_type_str","region","etp_year"], dropna=False).agg(
        alive_total_glb   = ("current_surviving_trees","sum"),
        sampled_total_glb = ("trees_contract","sum"),
        total_contracts   = ("contract_code","nunique"),
    ).reset_index()

    df_act = mbt[mbt["status"].isin(ACTIVE_STATUSES)].copy()
    g_act = df_act.groupby(["allocation_type_str","region","etp_year"], dropna=False).agg(
        alive_total_act   = ("current_surviving_trees","sum"),
        sampled_total_act = ("trees_contract","sum"),
        total_active      = ("contract_code","nunique"),
    ).reset_index()

    out = status_counts.merge(g_glb, on=["allocation_type_str","region","etp_year"], how="left") \
                       .merge(g_act, on=["allocation_type_str","region","etp_year"], how="left")

    out["Survival (Active)"] = out.apply(
        lambda r: fmt_pct_1d(r.get("alive_total_act"), r.get("sampled_total_act")), axis=1
    )
    out["Survival (Global)"] = out.apply(
        lambda r: fmt_pct_1d(r.get("alive_total_glb"), r.get("sampled_total_glb")), axis=1
    )

    out = out.rename(columns={
        "allocation_type_str": "Allocation Type",
        "region": "Region",
        "etp_year": "ETP Year",
        "total_contracts": "Total Contracts",
        #"total_active": "Total Active Contracts"
    })

    # 👈 AQUÍ: dropeamos los campos de cálculo
    out = out.drop(
        columns=[
            "alive_total_glb",
            "sampled_total_glb",
            "alive_total_act",
            "sampled_total_act",
            "total_active",
        ],
        errors="ignore"
    )

    out["ETP Year"] = out["ETP Year"].astype("Int64").astype("string")
    out.loc[out["ETP Year"].isin(["<NA>","nan"]), "ETP Year"] = "Not asigned yet"

    cat_order = pd.CategoricalDtype(categories=["COP","COP|ETP","ETP"], ordered=True)
    out["Allocation Type"] = out["Allocation Type"].astype(cat_order)

    fixed_left  = ["Allocation Type","Region","ETP Year", "Total Contracts"]
    fixed_right = ["Survival (Active)","Survival (Global)"]
    status_cols = [c for c in out.columns if c not in fixed_left + fixed_right]

    out = out.sort_values(by=["ETP Year","Allocation Type","Region"], na_position="last")
    out = out[fixed_left + status_cols + fixed_right].reset_index(drop=True)
    return out
