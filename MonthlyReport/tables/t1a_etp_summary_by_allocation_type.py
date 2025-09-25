# MonthlyReport/tables/t1a_etp_summary_by_allocation_type.py
# -*- coding: utf-8 -*-
"""
T1A - ETP Summary by Allocation Type
------------------------------------
Desagrega el conteo de contratos por tipo de asignación (COP, ETP, COP/ETP),
con pivot en columnas por status. Se usa principalmente para auditorías.
"""

from core.libs import pd, np
from MonthlyReport.utils_monthly_base import build_monthly_base_table
from MonthlyReport.tables_process import fmt_pct_1d, get_allocation_type, compute_allocation_type_contract
from core.db import get_engine

def build_etp_summary_by_allocation(engine=None) -> pd.DataFrame:
    if engine is None:
        engine = get_engine()

    # Base de contratos (mbt ya integra CTI + métricas)
    mbt = build_monthly_base_table()
    if mbt.empty:
        return pd.DataFrame()

    # Traer CA solo con columnas necesarias (evita sorpresas)
    ca = pd.read_sql("""
        SELECT
            contract_code,
            usa_trees_contracted,
            usa_trees_planted,
            canada_trees_contracted,
            total_can_allocation,
            canada_2017_trees
        FROM masterdatabase.contract_allocation
    """, engine)

    # Merge
    df = mbt.merge(ca, on="contract_code", how="left")

    # Normaliza posibles duplicados *_x/*_y heredados de mbt
    for col in ["usa_trees_contracted","usa_trees_planted","canada_trees_contracted","total_can_allocation","canada_2017_trees"]:
        if f"{col}_y" in df.columns:
            df[col] = df[f"{col}_y"]
        elif f"{col}_x" in df.columns:
            df[col] = df[f"{col}_x"]

    # --- Etiquetas por cohorte (contexto)
    df["allocation_type_year"] = df["etp_year"].apply(
        lambda y: "/".join(get_allocation_type(int(y))) if pd.notna(y) else "NA"
    )

    # --- Clasificación central para contratos (fuente CA o CTI según cohorte)
    df["allocation_type_contract"] = compute_allocation_type_contract(df)

    # =====================
    # 1) Conteo por estado
    # =====================
    status_counts = (
        df.groupby(
            ["etp_year","region","allocation_type_year","allocation_type_contract","status"],
            dropna=False
        )["contract_code"]
         .nunique()
         .unstack("status", fill_value=0)
         .reset_index()
    )

    # =====================
    # 2) Agregado global
    # =====================
    g_glb = (
        df.groupby(
            ["etp_year","region","allocation_type_year","allocation_type_contract"],
            dropna=False
        )
        .agg(
            alive_total_glb=("current_surviving_trees","sum"),
            sampled_total_glb=("trees_contract","sum"),
            total_contracts=("contract_code","nunique"),
        )
        .reset_index()
    )

    # =====================
    # 3) Agregado no-OOP
    # =====================
    df_non_oop = df[df["status"].fillna("").str.strip() != "Out of Program"].copy()

    # 👇 extra: excluir contratos marcados con filter
    if "filter" in df_non_oop.columns:
        df_non_oop = df_non_oop[df_non_oop["filter"].isna()].copy()

    g_non_oop = (
        df_non_oop.groupby(
            ["etp_year", "region", "allocation_type_year", "allocation_type_contract"],
            dropna=False
        )
        .agg(
            alive_total_non_oop=("current_surviving_trees", "sum"),
            sampled_total_non_oop=("trees_contract", "sum"),
            total_non_oop=("contract_code", "nunique"),
        )
        .reset_index()
    )

    # =========
    # 4) Merge
    # =========
    out = (
        status_counts
        .merge(g_glb,    on=["etp_year","region","allocation_type_year","allocation_type_contract"], how="left")
        .merge(g_non_oop,on=["etp_year","region","allocation_type_year","allocation_type_contract"], how="left")
    )

    # ==============================
    # 5) Survival (% no-OOP) bonito
    # ==============================
    out["Survival"] = np.where(
        out["total_non_oop"].fillna(0) > 0,
        out.apply(lambda r: fmt_pct_1d(r.get("alive_total_non_oop"), r.get("sampled_total_non_oop")), axis=1),
        None
    )

    # ============
    # 6) Limpieza
    # ============
    out = out.drop(columns=[
        "alive_total_glb","sampled_total_glb",
        "alive_total_non_oop","sampled_total_non_oop","total_non_oop"
    ], errors="ignore")

    out["etp_year"] = out["etp_year"].astype("Int64").astype("string")
    out.loc[out["etp_year"].isin(["<NA>","nan"]), "etp_year"] = "Not asigned yet"

    # Categorías ordenadas (incluye 'NA' para la etiqueta contractual)
    cat_year  = pd.CategoricalDtype(categories=["COP","COP/ETP","ETP"], ordered=True)
    cat_contr = pd.CategoricalDtype(categories=["COP","ETP","COP/ETP","NA"], ordered=True)
    out["allocation_type_year"]     = out["allocation_type_year"].astype(cat_year)
    out["allocation_type_contract"] = out["allocation_type_contract"].astype(cat_contr)

    # Orden y columnas finales
    fixed_left  = ["allocation_type_year","allocation_type_contract","region","etp_year","total_contracts"]
    fixed_right = ["Survival"]
    status_cols = [c for c in out.columns if c not in fixed_left + fixed_right]

    out = out.sort_values(
        by=["etp_year","allocation_type_year","allocation_type_contract","region"],
        na_position="last"
    ).reset_index(drop=True)

    out = out[fixed_left + status_cols + fixed_right]

    return out



