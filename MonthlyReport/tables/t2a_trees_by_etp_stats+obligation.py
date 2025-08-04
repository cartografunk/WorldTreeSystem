#MonthlyReport/tables/t2a_trees_by_etp_stats+obligation.py

from core.libs import pd
from MonthlyReport.tables_process import format_survival_summary

def enrich_with_obligations_and_stats(df, engine):
    # Asegura que etp_year esté presente para hacer merge
    df["etp_year"] = df["year"]

    # 📥 Cargar stats de supervivencia
    df_survival = df[df["contract_trees_status"] == "Surviving"].copy()
    df_survival["Survival_pct"] = df_survival["Total"] / df_survival.groupby("year")["Total"].transform("sum")

    stats = df_survival.groupby("etp_year")["Survival_pct"].agg(
        mean="mean",
        median="median",
        mode=lambda x: x.mode().iloc[0] if not x.mode().empty else None,
        max="max",
        min="min",
        range=lambda x: x.max() - x.min()
    ).reset_index()

    def fmt(p):
        return f"{round(p * 100, 1)}%" if pd.notna(p) else "N/A"

    stats["Survival_Stats"] = (
        "mean: " + stats["mean"].apply(fmt) + ", " +
        "median: " + stats["median"].apply(fmt) + ", " +
        "mode: " + stats["mode"].apply(fmt) + ", " +
        "max: " + stats["max"].apply(fmt) + ", " +
        "min: " + stats["min"].apply(fmt) + ", " +
        "range: " + stats["range"].apply(fmt)
    )
    stats = stats[["etp_year", "Survival_Stats"]]

    # 📥 series_obligation desde BDD
    series_ob = pd.read_sql("SELECT * FROM masterdatabase.series_obligation", engine)

    # 📥 resumen textual
    resumen = format_survival_summary(df)

    # 🔁 Aplicar a Surviving
    df_surviving = df[df["contract_trees_status"] == "Surviving"].copy()
    df_surviving = df_surviving.merge(stats, on="etp_year", how="left")
    df_surviving = df_surviving.merge(series_ob, on="etp_year", how="left")
    df_surviving = df_surviving.merge(resumen.rename(columns={"Resumen": "Survival_Summary"}), on="etp_year", how="left")

    contracted_totals = df[df["contract_trees_status"] == "Contracted"].set_index("etp_year")["Total"]
    df_surviving["Obligation_Remaining"] = (
        df_surviving["series_obligation"] - df_surviving["etp_year"].map(contracted_totals)
    ).round(0).astype("Int64")

    df_surviving.drop(columns=["series_obligation"], inplace=True)

    # Reunir
    df_others = df[df["contract_trees_status"] != "Surviving"].copy()
    df_final = pd.concat([df_others, df_surviving], ignore_index=True)

    # Reordenar
    ordered_cols = [col for col in df_final.columns if col != "Obligation_Remaining"] + ["Obligation_Remaining"]
    return df_final[ordered_cols]