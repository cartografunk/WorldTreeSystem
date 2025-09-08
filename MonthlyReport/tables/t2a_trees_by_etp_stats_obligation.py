# MonthlyReport/tables/t2a_trees_by_etp_stats_obligation.py

from core.libs import pd
from MonthlyReport.tables_process import clean_t2a_for_excel, get_allocation_type
from MonthlyReport.stats import survival_stats
# OJO: no usamos normalize_region_series aquí

def enrich_with_obligations_and_stats(df, engine):
    df = df.copy()

    # ===== Asegura etp_year / etiqueta etp coherente con T2 =====
    if "etp_year" not in df.columns:
        if "year" in df.columns:
            df["etp_year"] = df["year"]
        else:
            raise ValueError("El DataFrame de entrada debe tener 'year' o 'etp_year'.")

    df["etp_year"] = pd.to_numeric(df["etp_year"], errors="coerce").astype("Int64")

    # Etiqueta etp basada en reglas (COP / ETP / COP/ETP)
    df["etp_expected"] = df["etp_year"].apply(
        lambda y: "/".join(get_allocation_type(int(y))) if pd.notna(y) else pd.NA
    )
    if "etp" in df.columns:
        df["etp"] = df["etp_expected"].fillna(df["etp"].astype("string").str.strip())
    else:
        df["etp"] = df["etp_expected"]
    df.drop(columns=["etp_expected"], inplace=True)

    # ===== 1) Base por contrato para survival (CTI para etp_year/trees_contract, CFI para status) =====
    surv = pd.read_sql(
        """
        SELECT contract_code, current_surviving_trees, current_survival_pct
        FROM masterdatabase.survival_current
        """,
        engine,
    )

    meta = pd.read_sql(
        """
        SELECT
            cti.contract_code,
            cti.etp_year,
            cti.trees_contract,
            cti.status                -- 👈 desde CTI
        FROM masterdatabase.contract_tree_information cti
        """,
        engine,
    )

    meta["etp_year"] = pd.to_numeric(meta["etp_year"], errors="coerce").astype("Int64")
    meta["trees_contract"] = pd.to_numeric(meta["trees_contract"], errors="coerce")

    active = (
        meta.loc[meta["status"] == "Active"]
            .merge(surv, on="contract_code", how="left")
            .copy()
    )
    active["current_surviving_trees"] = pd.to_numeric(
        active["current_surviving_trees"], errors="coerce"
    ).fillna(0)

    # survival_pct en [0,1], por contrato, usando trees_contract como denominador
    active["survival_pct"] = active.apply(
        lambda r: (r["current_surviving_trees"] / r["trees_contract"])
        if pd.notna(r["trees_contract"]) and r["trees_contract"] > 0
        else pd.NA,
        axis=1,
    )

    # ===== 2) Stats por etp_year (NO ponderados) =====
    stats_num, stats_txt = survival_stats(
        df=active,
        group_col="etp_year",
        survival_pct_col="survival_pct",
    )
    stats_sel = (
        stats_num[["etp_year", "mean", "median", "mode", "max", "min", "range"]]
        if not stats_num.empty
        else stats_num
    )

    # ===== 3) Obligations por cohorte =====
    series_ob = pd.read_sql(
        "SELECT etp_year, series_obligation FROM masterdatabase.series_obligation",
        engine,
    )
    series_ob["etp_year"] = pd.to_numeric(series_ob["etp_year"], errors="coerce").astype("Int64")

    # ===== 4) Enriquecer SOLO filas 'Surviving' del DF base =====
    df_surviving = df[df["contract_trees_status"] == "Surviving"].copy()

    # 4.1) Adjunta stats numéricas y obligación por etp_year
    df_surviving = (
        df_surviving.merge(stats_sel, on="etp_year", how="left")
                    .merge(series_ob, on="etp_year", how="left")
    )

    # 4.2) Survival % poblacional por cohorte (a partir del propio DF base)
    tmp = df.copy()
    tmp["Total"] = pd.to_numeric(tmp["Total"], errors="coerce").fillna(0)

    totals_by = (
        tmp.groupby(["etp_year", "contract_trees_status"])["Total"]
           .sum(min_count=1)
           .unstack(fill_value=0)  # columnas: Contracted, Planted, Surviving (las que existan)
           .reset_index()
    )
    for col in ["Planted", "Surviving"]:
        if col not in totals_by.columns:
            totals_by[col] = 0

    totals_by["Survival %"] = (totals_by["Surviving"] / totals_by["Planted"]).where(totals_by["Planted"] > 0)
    rate = totals_by[["etp_year", "Survival %"]].copy()

    df_surviving = df_surviving.merge(rate, on="etp_year", how="left")

    # 4.3) Añadir resumen textual de stats (una fila por cohorte)
    stats_txt_dedup = stats_txt.drop_duplicates(subset=["etp_year"])
    df_surviving = df_surviving.merge(stats_txt_dedup, on="etp_year", how="left")

    # ===== 5) Cálculo de Obligation_Remaining =====
    contracted_totals = (
        df[df["contract_trees_status"] == "Contracted"]
          .groupby("etp_year")["Total"]
          .sum(min_count=1)
    )

    df_surviving["series_obligation"] = pd.to_numeric(df_surviving["series_obligation"], errors="coerce")
    df_surviving["Obligation_Remaining"] = (
        df_surviving["series_obligation"]
        - df_surviving["etp_year"].map(contracted_totals).fillna(0)
    )
    df_surviving["Obligation_Remaining"] = (
        df_surviving["Obligation_Remaining"]
          .where(pd.notna(df_surviving["Obligation_Remaining"]), pd.NA)
          .clip(lower=0)
          .round(0)
          .astype("Int64")
    )
    df_surviving.drop(columns=["series_obligation"], inplace=True)

    # ===== 6) Reunir y ordenar =====
    df_others = df[df["contract_trees_status"] != "Surviving"].copy()
    df_final = pd.concat([df_others, df_surviving], ignore_index=True)

    # Mueve Obligation_Remaining al final
    ordered_cols = [c for c in df_final.columns if c != "Obligation_Remaining"] + ["Obligation_Remaining"]
    df_final = df_final[ordered_cols]

    # Limpieza final y orden para Excel
    df_final = clean_t2a_for_excel(df_final)

    # Orden de filas
    order = ["Contracted", "Planted", "Surviving"]
    if "year" in df_final.columns:
        df_final["year"] = pd.to_numeric(df_final["year"], errors="coerce")
        sort_keys = ["year", "etp", "contract_trees_status"]
    else:
        # si no hay 'year', usa 'etp_year' para ordenar
        sort_keys = ["etp_year", "etp", "contract_trees_status"]

    if "contract_trees_status" in df_final.columns:
        df_final["contract_trees_status"] = pd.Categorical(
            df_final["contract_trees_status"], categories=order, ordered=True
        )

    df_final = df_final.sort_values(by=sort_keys, ascending=[True, True, True], ignore_index=True)

    # Evita duplicado visual de etp_year si no lo necesitas
    df_final = df_final.drop(columns=["etp_year"], errors="ignore")

    return df_final
