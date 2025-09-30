# MonthlyReport/tables/t2a_trees_by_etp_stats_obligation.py

from core.libs import pd
from MonthlyReport.tables_process import clean_t2a_for_excel, get_allocation_type, apply_aliases
from MonthlyReport.stats import survival_stats
from MonthlyReport.tables_process import compute_allocation_type_contract, _coerce_survival_pct
# OJO: no usamos normalize_region_series aquí

def enrich_with_obligations_and_stats(df, engine):
    df = df.copy()

    # ===== 1) Base de contratos (CTI + CA) =====
    cti = pd.read_sql("""
        SELECT contract_code, etp_year, trees_contract, planted, status, "Filter"
        FROM masterdatabase.contract_tree_information
    """, engine)

    ca = pd.read_sql("SELECT * FROM masterdatabase.contract_allocation", engine)

    contracts = cti.merge(ca, on="contract_code", how="left")

    # ===== 2) Allocation type coherente con T2 =====
    contracts["allocation_type"] = compute_allocation_type_contract(contracts)

    # ===== 3) Survival (con funciones de tables_process) =====
    surv = pd.read_sql("""
        SELECT contract_code, current_surviving_trees, current_survival_pct
        FROM masterdatabase.survival_current
    """, engine)

    contracts = contracts.merge(surv, on="contract_code", how="left")
    contracts["survival_pct"] = _coerce_survival_pct(contracts)

    records = []

    for y, grp in contracts.groupby("etp_year"):
        alloc = grp["allocation_type"].iloc[0]  # COP / ETP / COP/ETP

        contracted = grp["trees_contract"].sum(min_count=1)
        planted = grp["planted"].sum(min_count=1)
        surviving = grp["current_surviving_trees"].sum(min_count=1)

        records.append({"etp_year": y, "etp": alloc,
                        "contract_trees_status": "Contracted", "Total": contracted})
        records.append({"etp_year": y, "etp": alloc,
                        "contract_trees_status": "Planted", "Total": planted})
        records.append({"etp_year": y, "etp": alloc,
                        "contract_trees_status": "Surviving", "Total": surviving})

    df = pd.DataFrame(records)

    # ===== 4) Stats numéricos y resumen textual =====
    stats_num, stats_txt = survival_stats(
        df=contracts,
        group_col="etp_year",
        survival_pct_col="survival_pct",
    )

    # ===== 5) Series obligation =====
    series_ob = pd.read_sql(
        "SELECT etp_year, series_obligation FROM masterdatabase.series_obligation",
        engine,
    )
    series_ob["etp_year"] = pd.to_numeric(series_ob["etp_year"], errors="coerce").astype("Int64")

    # ===== 6) Enriquecer filas 'Surviving' de df base =====
    df_surv = df[df["contract_trees_status"] == "Surviving"].copy()
    df_surv = (
        df_surv.merge(stats_num, on="etp_year", how="left")
               .merge(series_ob, on="etp_year", how="left")
               .merge(stats_txt.drop_duplicates("etp_year"), on="etp_year", how="left")
    )

    # calcular Obligation_Remaining
    contracted_totals = (
        df[df["contract_trees_status"] == "Contracted"]
          .groupby("etp_year")["Total"].sum(min_count=1)
    )
    df_surv["Obligation_Remaining"] = (
        df_surv["series_obligation"].fillna(0)
        - df_surv["etp_year"].map(contracted_totals).fillna(0)
    ).clip(lower=0).astype("Int64")

    df_surv.drop(columns=["series_obligation"], inplace=True)

    # ===== 7) Reunir =====
    df_final = pd.concat([df[df["contract_trees_status"] != "Surviving"], df_surv],
                         ignore_index=True)

    # limpieza con función oficial
    df_final = clean_t2a_for_excel(df_final)

    # ===== 8) Orden =====
    order = ["Contracted", "Planted", "Surviving"]
    if "contract_trees_status" in df_final.columns:
        df_final["contract_trees_status"] = pd.Categorical(
            df_final["contract_trees_status"], categories=order, ordered=True
        )
    df_final = df_final.sort_values(
        by=["etp_year", "etp", "contract_trees_status"],
        ascending=[True, True, True], ignore_index=True
    )

    return df_final
