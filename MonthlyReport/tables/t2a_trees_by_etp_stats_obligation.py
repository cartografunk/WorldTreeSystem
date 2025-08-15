# MonthlyReport/tables/t2a_trees_by_etp_stats_obligation.py

from core.libs import pd
from MonthlyReport.tables_process import clean_t2a_for_excel, get_allocation_type

def enrich_with_obligations_and_stats(df, engine):
    df = df.copy()

    # Asegura etp_year
    if "etp_year" not in df.columns:
        if "year" in df.columns:
            df["etp_year"] = df["year"]
        else:
            raise ValueError("El DataFrame de entrada debe tener 'year' o 'etp_year'.")

    # 🔧 Normaliza la etiqueta 'etp' con la misma lógica de T2
    df["etp_year"] = pd.to_numeric(df["etp_year"], errors="coerce").astype("Int64")
    df["etp_expected"] = df["etp_year"].apply(
        lambda y: "/".join(get_allocation_type(int(y))) if pd.notna(y) else pd.NA
    )
    if "etp" in df.columns:
        df["etp"] = df["etp_expected"].fillna(df["etp"].astype("string").str.strip())
    else:
        df["etp"] = df["etp_expected"]
    df.drop(columns=["etp_expected"], inplace=True)

    # ===== 1) Base por contrato para survival (solo contratos activos) =====
    surv = pd.read_sql("""
        SELECT contract_code, current_surviving_trees, current_survival_pct
        FROM masterdatabase.survival_current
    """, engine)

    # ===== 1) Base por contrato para survival (solo contratos activos) =====
    surv = pd.read_sql("""
        SELECT contract_code, current_surviving_trees, current_survival_pct
        FROM masterdatabase.survival_current
    """, engine)

    meta = pd.read_sql("""
        SELECT
            cti.contract_code,
            cti.etp_year,
            cti.trees_contract,
            cfi.status
        FROM masterdatabase.contract_tree_information cti
        LEFT JOIN masterdatabase.contract_farmer_information cfi
          ON cti.contract_code = cfi.contract_code
    """, engine)

    meta["etp_year"] = pd.to_numeric(meta["etp_year"], errors="coerce").astype("Int64")
    meta["trees_contract"] = pd.to_numeric(meta["trees_contract"], errors="coerce")

    active = (
        meta.loc[meta["status"] == "Active"]
            .merge(surv, on="contract_code", how="left")
            .copy()
    )
    active["current_surviving_trees"] = pd.to_numeric(active["current_surviving_trees"], errors="coerce").fillna(0)

    # survival_pct en [0,1]
    active["survival_pct"] = active.apply(
        lambda r: (r["current_surviving_trees"] / r["trees_contract"])
        if pd.notna(r["trees_contract"]) and r["trees_contract"] > 0 else pd.NA,
        axis=1
    )

    # ===== 2) Stats por etp_year (numéricas y texto) =====
    def _mode_safe(s):
        m = s.mode(dropna=True)
        return m.iloc[0] if not m.empty else pd.NA

    stats_num = (
        active.dropna(subset=["survival_pct"])
              .groupby("etp_year")["survival_pct"]
              .agg(mean="mean", median="median", mode=_mode_safe, max="max", min="min")
              .reset_index()
    )
    if not stats_num.empty:
        stats_num["range"] = stats_num["max"] - stats_num["min"]

    def _fmt(p):
        try:
            return f"{round(float(p) * 100, 1)}%" if pd.notna(p) else "N/A"
        except Exception:
            return "N/A"

    if not stats_num.empty:
        stats_txt = stats_num[["etp_year"]].copy()
        stats_txt["Survival_Summary"] = (
            "mean: "   + stats_num["mean"].apply(_fmt)   + ", " +
            "median: " + stats_num["median"].apply(_fmt) + ", " +
            "mode: "   + stats_num["mode"].apply(_fmt)   + ", " +
            "max: "    + stats_num["max"].apply(_fmt)    + ", " +
            "min: "    + stats_num["min"].apply(_fmt)    + ", " +
            "range: "  + stats_num["range"].apply(_fmt)
        )
    else:
        stats_txt = pd.DataFrame(columns=["etp_year", "Survival_Summary"])

    # ===== 2.1) Survival = Surviving / Planted por etp_year =====
    surviving_totals = (
        df[df["contract_trees_status"] == "Surviving"]
        .groupby("etp_year")["Total"]
        .sum(min_count=1)
    )
    planted_totals = (
        df[df["contract_trees_status"] == "Planted"]
        .groupby("etp_year")["Total"]
        .sum(min_count=1)
    )

    survival_ratio = (surviving_totals / planted_totals)

    # Agregamos Survival al stats_txt
    stats_txt["Survival"] = stats_txt["etp_year"].map(survival_ratio)

    # ===== 3) Obligations por serie =====
    series_ob = pd.read_sql(
        "SELECT etp_year, series_obligation FROM masterdatabase.series_obligation",
        engine
    )
    series_ob["etp_year"] = pd.to_numeric(series_ob["etp_year"], errors="coerce").astype("Int64")

    # ===== 4) Aplicar a filas 'Surviving' del df base =====
    df_surviving = df[df["contract_trees_status"] == "Surviving"].copy()

    stats_sel = stats_num[["etp_year", "mean", "median", "mode", "max", "min", "range"]] \
                if not stats_num.empty else stats_num

    df_surviving = (
        df_surviving
            .merge(stats_sel, on="etp_year", how="left")
            .merge(series_ob, on="etp_year", how="left")
            .merge(stats_txt.drop_duplicates(subset=["etp_year"]), on="etp_year", how="left")
    )

    # 🧹 Fix para eliminar columna duplicada etp_year
    if "etp_year_y" in df_surviving.columns:
        df_surviving.drop(columns=["etp_year_y"], inplace=True)
    if "etp_year_x" in df_surviving.columns:
        df_surviving.rename(columns={"etp_year_x": "etp_year"}, inplace=True)

    # ===== 5) Contracted totals por etp_year (desde el mismo df) =====
    contracted_totals = (
        df[df["contract_trees_status"] == "Contracted"]
          .groupby("etp_year")["Total"]
          .sum(min_count=1)
    )

    df_surviving["series_obligation"] = pd.to_numeric(df_surviving["series_obligation"], errors="coerce")
    df_surviving["Obligation_Remaining"] = (
        df_surviving["series_obligation"] - df_surviving["etp_year"].map(contracted_totals).fillna(0)
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

    ordered_cols = [c for c in df_final.columns if c != "Obligation_Remaining"] + ["Obligation_Remaining"]
    df_final = df_final[ordered_cols]

    # 👇 limpieza final: drop de mean/median/mode/max/min/range y polish opcional
    df_final = clean_t2a_for_excel(df_final)  # strip_etp_tags=True por defecto si usaste mi versión

    # Orden final: por year, etp y contract_trees_status ASC
    order = ["Contracted", "Planted", "Surviving"]

    # Asegura tipos
    if "year" in df_final.columns:
        df_final["year"] = pd.to_numeric(df_final["year"], errors="coerce")
    if "contract_trees_status" in df_final.columns:
        df_final["contract_trees_status"] = pd.Categorical(
            df_final["contract_trees_status"], categories=order, ordered=True
        )

    df_final = df_final.sort_values(
        by=["year", "etp", "contract_trees_status"],
        ascending=[True, True, True],
        ignore_index=True
    )

    return df_final

