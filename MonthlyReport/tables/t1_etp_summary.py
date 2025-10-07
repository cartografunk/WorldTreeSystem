# MonthlyReport/tables/t1_etp_summary.py
from core.libs import pd, np
from MonthlyReport.utils_monthly_base import build_monthly_base_table
from MonthlyReport.tables_process import fmt_pct_1d

def build_etp_summary(engine=None) -> pd.DataFrame:
    mbt = build_monthly_base_table()

    # ==============================================================
    # 🔧 FIX LÓGICO PARA COHORTES ESPECIALES (2017 COP PURO)
    # ==============================================================

    # Crear columna auxiliar de agrupación (no sobreescribe etp_year real)
    if "canada_2017_trees" in mbt.columns:
        mbt["etp_year_logic"] = np.where(
            mbt["canada_2017_trees"].fillna(0) > 0,
            2017,
            mbt["etp_year"]
        )
    else:
        mbt["etp_year_logic"] = mbt["etp_year"]

    # --- FIX adicional: 2017 es COP puro, ajustar tipo lógico ---
    mbt["etp_type_logic"] = np.where(
        (mbt["canada_2017_trees"].fillna(0) > 0) &
        (mbt["etp_type"].isin(["ETP/COP", "ETP"])),
        "COP",
        mbt["etp_type"]
    )

    # Si hay contratos canadienses 2017, elimina duplicaciones entre años
    mask_2017 = mbt["canada_2017_trees"].fillna(0) > 0
    if mask_2017.any():
        codes_2017 = mbt.loc[mask_2017, "contract_code"].unique()
        # Mantener solo la versión lógica 2017 de esos contratos
        mbt = mbt[~mbt["contract_code"].isin(codes_2017) | mask_2017].copy()

    # Definimos qué columnas usar para agrupar
    year_col = "etp_year_logic"
    type_col = "etp_type_logic"

    # ==============================================================
    # 1) Conteo por estado (para columnas wide)
    # ==============================================================
    status_counts = (
        mbt.groupby([type_col, "region", year_col, "status"], dropna=False)["contract_code"]
           .nunique()
           .unstack("status", fill_value=0)
           .reset_index()
    )

    # ==============================================================
    # 2) Agregado global (todos)
    # ==============================================================
    g_glb = mbt.groupby([type_col, "region", year_col], dropna=False).agg(
        alive_total_glb=("current_surviving_trees", "sum"),
        sampled_total_glb=("trees_contract", "sum"),
        total_contracts=("contract_code", "nunique"),
    ).reset_index()

    # ==============================================================
    # 3) Agregado no-OOP (todos menos Out of Program + Filter)
    # ==============================================================
    df_non_oop = mbt[mbt["status"].fillna("").str.strip() != "Out of Program"].copy()
    if "Filter" in df_non_oop.columns:
        df_non_oop = df_non_oop[df_non_oop["Filter"].isna()].copy()

    g_non_oop = df_non_oop.groupby(
        [type_col, "region", year_col], dropna=False
    ).agg(
        alive_total_non_oop=("current_surviving_trees", "sum"),
        sampled_total_non_oop=("trees_contract", "sum"),
        total_non_oop=("contract_code", "nunique"),
    ).reset_index()

    # ==============================================================
    # 4) Merge y survival
    # ==============================================================
    out = (
        status_counts
        .merge(g_glb, on=[type_col, "region", year_col], how="left")
        .merge(g_non_oop, on=[type_col, "region", year_col], how="left")
    )

    out["Survival"] = np.where(
        out["total_non_oop"].fillna(0) > 0,
        out.apply(lambda r: fmt_pct_1d(
            r.get("alive_total_non_oop"), r.get("sampled_total_non_oop")
        ), axis=1),
        None
    )

    # ==============================================================
    # 5) Limpieza/renombres
    # ==============================================================
    out = out.rename(columns={
        type_col: "Allocation Type",
        "region": "Region",
        year_col: "ETP Year",
        "total_contracts": "Total Contracts",
    })

    out = out.drop(columns=[
        "alive_total_glb", "sampled_total_glb",
        "alive_total_non_oop", "sampled_total_non_oop", "total_non_oop"
    ], errors="ignore")

    out["ETP Year"] = out["ETP Year"].astype("Int64").astype("string")
    out.loc[out["ETP Year"].isin(["<NA>", "nan"]), "ETP Year"] = "Not asigned yet"

    cat_order = pd.CategoricalDtype(categories=["COP", "ETP/COP", "ETP"], ordered=True)
    out["Allocation Type"] = out["Allocation Type"].astype(cat_order)

    fixed_left  = ["Allocation Type", "Region", "ETP Year", "Total Contracts"]
    fixed_right = ["Survival"]
    status_cols = [c for c in out.columns if c not in fixed_left + fixed_right]

    out = out.sort_values(by=["ETP Year", "Allocation Type", "Region"], na_position="last")
    out = out[fixed_left + status_cols + fixed_right].reset_index(drop=True)
    return out
