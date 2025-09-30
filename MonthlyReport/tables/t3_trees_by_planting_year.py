# MonthlyReport/tables/t3_trees_by_planting_year.py

from core.libs import pd
from MonthlyReport.utils_monthly_base import build_monthly_base_table
from MonthlyReport.tables_process import get_allocation_type
from MonthlyReport.stats import survival_stats

DISPLAY_MAP  = {"CR": "Costa Rica", "GT": "Guatemala", "MX": "Mexico", "US": "USA"}
COUNTRIES    = ["Costa Rica", "Guatemala", "Mexico", "USA"]

def build_t3_trees_by_planting_year(engine):
    # === 1) Base única y tipado ===
    mbt = build_monthly_base_table()
    if mbt.empty:
        return pd.DataFrame(columns=["Year","Row"] + COUNTRIES + ["Total","Survival %","Survival_Summary"])

    mbt["etp_year"]       = pd.to_numeric(mbt.get("etp_year"), errors="coerce").astype("Int64")
    mbt["trees_contract"] = pd.to_numeric(mbt.get("trees_contract"), errors="coerce").fillna(0)
    mbt["alive_sc"] = pd.to_numeric(mbt.get("alive_sc"), errors="coerce").fillna(0).round(0).astype(int)
    mbt["planted"] = pd.to_numeric(mbt.get("planted"), errors="coerce").fillna(0).round(0).astype(int)
    mbt["planted_cop"] = pd.to_numeric(mbt.get("planted_cop"), errors="coerce").fillna(0).round(0).astype(int)
    mbt["trees_contract"] = pd.to_numeric(mbt.get("trees_contract"), errors="coerce").fillna(0).round(0).astype(int)

    if "has_cop" not in mbt.columns:
        mbt["has_cop"] = False

    # Región visible (acepta códigos CR/GT/MX/US o ya nombres)
    mbt["region_disp"] = mbt.get("region").replace(DISPLAY_MAP).astype("string")

    # planting_year: planting_year -> planting_date.year -> etp_year
    py = pd.to_numeric(mbt.get("planting_year"), errors="coerce").astype("Int64")
    if "planting_date" in mbt.columns:
        dt = pd.to_datetime(mbt["planting_date"], errors="coerce")
        py = py.fillna(dt.dt.year.astype("Int64"))
    py = py.fillna(mbt["etp_year"])
    mbt["planting_year"] = py.astype("Int64")

    # === 2) Etiqueta de cohorte (COP / ETP / COP/ETP) por etp_year ===
    years = sorted(mbt["etp_year"].dropna().astype(int).unique().tolist())
    alloc_map = {y: "/".join(get_allocation_type(int(y))) for y in years}
    mbt["allocation_type"] = mbt["etp_year"].apply(lambda y: alloc_map.get(int(y)) if pd.notna(y) else pd.NA)

    # === 3) Planted/Surviving con reglas alineadas a T2 ===
    mbt["Planted_use"] = 0.0
    cop = mbt["allocation_type"].eq("COP")
    etp = mbt["allocation_type"].eq("ETP")
    mix = mbt["allocation_type"].eq("COP/ETP")

    # ETP
    mbt.loc[etp, "Planted_use"] = mbt.loc[etp, "planted"]
    # COP (solo contratos con COP)
    mbt.loc[cop & mbt["has_cop"], "Planted_use"] = mbt.loc[cop & mbt["has_cop"], "planted_cop"]
    # MIX: COP si tiene, si no ETP
    mbt.loc[mix, "Planted_use"] = mbt.loc[mix, "planted_cop"].where(mbt.loc[mix, "has_cop"], mbt.loc[mix, "planted"])

    mbt["Surviving_use"] = mbt["alive_sc"].clip(lower=0)
    mbt["Surviving_use"] = mbt[["Surviving_use", "Planted_use"]].min(axis=1)

    # === 4) Agregado por planting_year x país ===
    g = (
        mbt.groupby(["planting_year", "region_disp"], dropna=False)
           .agg(Planted=("Planted_use", "sum"),
                Surviving=("Surviving_use", "sum"))
           .reset_index()
    )

    def _pivot_sum(col):
        t = g.pivot_table(index="planting_year", columns="region_disp", values=col, aggfunc="sum", fill_value=0).reset_index()
        for c in COUNTRIES:
            if c not in t.columns:
                t[c] = 0
        t["Total"] = t[COUNTRIES].sum(axis=1)
        return t

    long_planted = _pivot_sum("Planted")
    long_surv    = _pivot_sum("Surviving")

    # === 5) Survival % poblacional por planting_year ===
    # 👇 survival base: excluir Out of Program + Filter
    mask_surv = (mbt["status"].fillna("").str.strip() != "Out of Program") & (mbt["Filter"].isna())
    g_planted = mbt.groupby(["planting_year", "region_disp"], dropna=False)["Planted_use"].sum().reset_index()
    g_surv = mbt.loc[mask_surv].groupby(["planting_year", "region_disp"], dropna=False)[
        "Surviving_use"].sum().reset_index()

    def _pivot_sum(df, col):
        t = df.pivot_table(index="planting_year", columns="region_disp", values=col, aggfunc="sum",
                           fill_value=0).reset_index()
        for c in COUNTRIES:
            if c not in t.columns:
                t[c] = 0
        t["Total"] = t[COUNTRIES].sum(axis=1)
        return t

    long_planted = _pivot_sum(g_planted, "Planted_use")
    long_surv = _pivot_sum(g_surv, "Surviving_use")

    rate = (
        long_planted[["planting_year", "Total"]].rename(columns={"Total": "P"})
        .merge(long_surv[["planting_year", "Total"]].rename(columns={"Total": "S"}), on="planting_year", how="outer")
        .fillna(0)
    )
    rate["Survival %"] = (rate["S"] / rate["P"]).where(rate["P"] > 0)

    # === 6) Survival Summary por planting_year (no ponderado) usando mbt ===
    base_stats = mbt.copy()
    if "status" in base_stats.columns:
        base_stats = base_stats[(base_stats["status"] == "Active") & (base_stats["Filter"].isna())]
    base_stats["survival_pct"] = base_stats.apply(
        lambda r: (r["alive_sc"] / r["trees_contract"]) if pd.notna(r["trees_contract"]) and r[
            "trees_contract"] > 0 else pd.NA,
        axis=1
    )

    stats_num, stats_txt = survival_stats(
        df=base_stats,
        group_col="planting_year",
        survival_pct_col="survival_pct",
    )
    summary_map = {}
    if stats_txt is not None and not stats_txt.empty:
        summary_map = dict(
            stats_txt.dropna(subset=["planting_year"]).set_index("planting_year")["Survival_Summary"]
        )

    # === 7) Construcción de filas Planted/Surviving por año ===
    lp = long_planted.set_index("planting_year")
    ls = long_surv.set_index("planting_year")
    rt = rate.set_index("planting_year")

    rows = []
    all_years = sorted(set(lp.index).union(ls.index))
    for y in all_years:
        # Planted row
        p = lp.loc[y] if y in lp.index else None
        planted_vals = [int(p.get(c, 0)) if p is not None else 0 for c in COUNTRIES]
        planted_total = int(p["Total"]) if p is not None else 0
        rows.append([int(y), "Planted", *planted_vals, planted_total, "", ""])

        # Surviving row
        s = ls.loc[y] if y in ls.index else None
        surv_vals = [int(s.get(c, 0)) if s is not None else 0 for c in COUNTRIES]
        surv_total = int(s["Total"]) if s is not None else 0
        surv_pct = rt.loc[y, "Survival %"] if y in rt.index else pd.NA
        surv_pct_str = f"{round(float(surv_pct)*100,1)}%" if pd.notna(surv_pct) else ""
        rows.append([int(y), "Surviving", *surv_vals, surv_total, surv_pct_str, summary_map.get(y, pd.NA)])

    out = pd.DataFrame(rows, columns=["Year", "Row"] + COUNTRIES + ["Total", "Survival %", "Survival_Summary"])

    # === 8) Footer de totales ===
    total_plan = out.loc[out["Row"] == "Planted", "Total"].sum()
    total_surv = out.loc[out["Row"] == "Surviving", "Total"].sum()
    footer = pd.DataFrame(
        [
            ["", "Total Planted",   *[""]*len(COUNTRIES), int(total_plan), "", ""],
            ["", "Total Surviving", *[""]*len(COUNTRIES), int(total_surv), "", ""],
        ],
        columns=["Year", "Row"] + COUNTRIES + ["Total", "Survival %", "Survival_Summary"]
    )

    out = pd.concat([out, footer], ignore_index=True)
    return out
