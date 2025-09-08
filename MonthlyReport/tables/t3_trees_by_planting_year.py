from core.libs import pd
from MonthlyReport.tables_process import get_allocation_type
from MonthlyReport.stats import survival_stats
from core.region import region_from_code  # ← derivamos país del code

def build_t3_trees_by_planting_year(engine):
    # ---- Bases (solo lo necesario)
    cti = pd.read_sql("""
        SELECT
            contract_code,
            etp_year,
            status,
            trees_contract,       -- p/ survival_pct
            planted,
            planting_year,
            planting_date
        FROM masterdatabase.contract_tree_information
    """, engine)

    ca = pd.read_sql("""
        SELECT contract_code, usa_trees_planted, total_can_allocation
        FROM masterdatabase.contract_allocation
    """, engine)

    sc = pd.read_sql("""
        SELECT contract_code, current_surviving_trees
        FROM masterdatabase.survival_current
    """, engine)

    # ---- Normaliza/deriva
    cti["etp_year"] = pd.to_numeric(cti["etp_year"], errors="coerce").astype("Int64")
    cti["trees_contract"] = pd.to_numeric(cti["trees_contract"], errors="coerce")
    cti["planted"] = pd.to_numeric(cti["planted"], errors="coerce")

    # Región 100% desde contract_code (no dependemos de FPI)
    cti["region"] = cti["contract_code"].map(region_from_code).astype("string")

    # planting_year: prioridad planting_year -> planting_date.year -> etp_year
    py = pd.to_numeric(cti.get("planting_year"), errors="coerce").astype("Int64")
    if "planting_date" in cti.columns:
        dt = pd.to_datetime(cti["planting_date"], errors="coerce")
        py = py.fillna(dt.dt.year.astype("Int64"))
    py = py.fillna(cti["etp_year"])
    cti["planting_year"] = py.astype("Int64")

    # ---- Lado (US/CAN) por cohorte y región (para planted_calc)
    def side(row):
        y = row["etp_year"]
        alloc = get_allocation_type(int(y)) if pd.notna(y) else []
        if alloc == ["ETP"]:
            return "US"
        if alloc == ["COP"]:
            return "CAN"
        if alloc == ["COP", "ETP"]:
            return "US" if str(row.get("region", "")).strip().upper() == "USA" else "CAN"
        return None

    base_alloc = cti.merge(ca, on="contract_code", how="left")
    base_alloc["side"] = base_alloc.apply(side, axis=1)

    def planted_val(r):
        if r["side"] == "US":
            return r["usa_trees_planted"] if pd.notna(r["usa_trees_planted"]) else r["planted"]
        if r["side"] == "CAN":
            return r["total_can_allocation"]
        return None

    base_alloc["planted_calc"] = pd.to_numeric(
        base_alloc.apply(planted_val, axis=1), errors="coerce"
    ).fillna(0)

    # ---- Base para stats (Active + survival_pct por contrato)
    base_stats = cti.merge(sc, on="contract_code", how="left")
    base_stats = base_stats[base_stats["status"] == "Active"].copy()
    base_stats["current_surviving_trees"] = pd.to_numeric(
        base_stats["current_surviving_trees"], errors="coerce"
    ).fillna(0)

    base_stats["survival_pct"] = base_stats.apply(
        lambda r: (r["current_surviving_trees"] / r["trees_contract"])
        if pd.notna(r["trees_contract"]) and r["trees_contract"] > 0 else pd.NA,
        axis=1
    )

    # ---- Stats (NO ponderadas) por planting_year
    stats_num, stats_txt = survival_stats(
        df=base_stats,
        group_col="planting_year",
        survival_pct_col="survival_pct"
    )
    summary_map = dict(
        stats_txt.dropna(subset=["planting_year"])
                 .set_index("planting_year")["Survival_Summary"]
    )

    # ---- Agrega por planting_year x región (Planted / Surviving)
    planted_year_region = (
        base_alloc.groupby(["planting_year","region"], dropna=False)["planted_calc"]
                  .sum(min_count=1)
                  .reset_index()
                  .rename(columns={"planted_calc":"Planted"})
    )

    surviving_year_region = (
        base_stats.groupby(["planting_year","region"], dropna=False)["current_surviving_trees"]
                  .sum(min_count=1)
                  .reset_index()
                  .rename(columns={"current_surviving_trees":"Surviving"})
    )

    # Full grid
    df = pd.merge(planted_year_region, surviving_year_region,
                  on=["planting_year","region"], how="outer").fillna(0)

    # Solo países target
    country_order = ["Costa Rica","Guatemala","Mexico","USA"]
    df["region"] = df["region"].astype("string")
    df = df[df["region"].isin(country_order)]

    # ---- Larga: dos filas por año (Planted / Surviving)
    long_planted = df.pivot_table(index="planting_year", columns="region", values="Planted", aggfunc="sum", fill_value=0).reset_index()
    long_surv    = df.pivot_table(index="planting_year", columns="region", values="Surviving", aggfunc="sum", fill_value=0).reset_index()

    # Asegura columnas y Totales
    for tbl in (long_planted, long_surv):
        for c in country_order:
            if c not in tbl.columns:
                tbl[c] = 0
    long_planted["Total"] = long_planted[country_order].sum(axis=1)
    long_surv["Total"]    = long_surv[country_order].sum(axis=1)

    # Survival % poblacional por cohorte
    rate = pd.merge(
        long_planted[["planting_year","Total"]].rename(columns={"Total":"P"}),
        long_surv[["planting_year","Total"]].rename(columns={"Total":"S"}),
        on="planting_year", how="outer"
    ).fillna(0)
    rate["Survival %"] = (rate["S"] / rate["P"]).where(rate["P"] > 0)

    # ---- Construcción final
    rows = []
    idx_lp = long_planted.set_index("planting_year")
    idx_ls = long_surv.set_index("planting_year")
    idx_rt = rate.set_index("planting_year")

    for y in sorted(set(idx_lp.index).union(idx_ls.index)):
        lp = idx_lp.loc[y] if y in idx_lp.index else pd.Series({c: 0 for c in country_order} | {"Total": 0})
        ls = idx_ls.loc[y] if y in idx_ls.index else pd.Series({c: 0 for c in country_order} | {"Total": 0})
        surv_pct = float(idx_rt.loc[y, "Survival %"]) if y in idx_rt.index else 0.0
        summary_txt = summary_map.get(y, pd.NA)

        planted_row = [int(y), "Planted"] + [int(lp.get(c, 0)) for c in country_order] + [int(lp.get("Total", 0)), "", ""]
        surviving_row = [int(y), "Surviving"] + [int(ls.get(c, 0)) for c in country_order] \
                        + [int(ls.get("Total", 0)), f"{round(surv_pct*100,1)}%", summary_txt]

        rows.extend([planted_row, surviving_row])

    cols = ["Year","Row"] + country_order + ["Total","Survival %","Survival_Summary"]
    out = pd.DataFrame(rows, columns=cols)

    # Footer totales
    total_plan = out[out["Row"]=="Planted"]["Total"].sum()
    total_surv = out[out["Row"]=="Surviving"]["Total"].sum()
    footer = pd.DataFrame([
        ["", "Total Planted"]   + [""]*len(country_order) + [int(total_plan), "", ""],
        ["", "Total Surviving"] + [""]*len(country_order) + [int(total_surv), "", ""],
    ], columns=cols)

    out = pd.concat([out, footer], ignore_index=True)
    return out
