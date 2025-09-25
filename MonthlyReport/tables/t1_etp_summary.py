#MonthlyReport/tables/t1_etp_summary.py
from core.libs import pd, np
from MonthlyReport.utils_monthly_base import build_monthly_base_table
from MonthlyReport.tables_process import get_allocation_type, fmt_pct_1d

def build_etp_summary(engine=None) -> pd.DataFrame:
    mbt = build_monthly_base_table()

    # 1) Conteo por estado (para columnas wide)
    status_counts = (
        mbt.groupby(["allocation_type_str","region","etp_year","status"], dropna=False)["contract_code"]
           .nunique()
           .unstack("status", fill_value=0)
           .reset_index()
    )

    # 2) Agregado global (todos)
    g_glb = mbt.groupby(["allocation_type_str","region","etp_year"], dropna=False).agg(
        alive_total_glb   = ("current_surviving_trees","sum"),
        sampled_total_glb = ("trees_contract","sum"),
        total_contracts   = ("contract_code","nunique"),
    ).reset_index()

    # 3) Agregado **no-OOP** (todos menos Out of Program)
    df_non_oop = mbt[mbt["status"].fillna("").str.strip() != "Out of Program"].copy()

    # 👇 nuevo: excluir también los marcados con filter
    if "Filter" in df_non_oop.columns:
        df_non_oop = df_non_oop[df_non_oop["Filter"].isna()].copy()

    g_non_oop = df_non_oop.groupby(
        ["allocation_type_str", "region", "etp_year"], dropna=False
    ).agg(
        alive_total_non_oop=("current_surviving_trees", "sum"),
        sampled_total_non_oop=("trees_contract", "sum"),
        total_non_oop=("contract_code", "nunique"),
    ).reset_index()

    # 4) Merge
    out = (
        status_counts
        .merge(g_glb,    on=["allocation_type_str","region","etp_year"], how="left")
        .merge(g_non_oop,on=["allocation_type_str","region","etp_year"], how="left")
    )

    # 5) Survival = porcentaje sobre **no-OOP** (si hay alguno)
    out["Survival"] = np.where(
        out["total_non_oop"].fillna(0) > 0,
        out.apply(lambda r: fmt_pct_1d(r.get("alive_total_non_oop"), r.get("sampled_total_non_oop")), axis=1),
        None
    )

    # 6) Limpieza/renombres
    out = out.rename(columns={
        "allocation_type_str": "Allocation Type",
        "region": "Region",
        "etp_year": "ETP Year",
        "total_contracts": "Total Contracts",
    })

    out = out.drop(columns=[
        "alive_total_glb","sampled_total_glb",
        "alive_total_non_oop","sampled_total_non_oop","total_non_oop"
    ], errors="ignore")

    out["ETP Year"] = out["ETP Year"].astype("Int64").astype("string")
    out.loc[out["ETP Year"].isin(["<NA>","nan"]), "ETP Year"] = "Not asigned yet"

    cat_order = pd.CategoricalDtype(categories=["COP","COP|ETP","ETP"], ordered=True)
    out["Allocation Type"] = out["Allocation Type"].astype(cat_order)

    fixed_left  = ["Allocation Type","Region","ETP Year","Total Contracts"]
    fixed_right = ["Survival"]
    status_cols = [c for c in out.columns if c not in fixed_left + fixed_right]

    out = out.sort_values(by=["ETP Year","Allocation Type","Region"], na_position="last")
    out = out[fixed_left + status_cols + fixed_right].reset_index(drop=True)
    return out
