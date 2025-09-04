# MonthlyReport/tables/t3_trees_by_planting_year.py
from core.libs import pd
from MonthlyReport.tables_process import get_allocation_type
from MonthlyReport.stats import survival_stats  # helper

def build_t3_trees_by_planting_year(engine):
    # ---- Bases
    cti = pd.read_sql("""
        SELECT
            contract_code,
            etp_year,
            trees_contract,          -- para survival_pct
            planted,
            planting_year,
            planting_date
        FROM masterdatabase.contract_tree_information
    """, engine)

    fpi = pd.read_sql("""
        SELECT contract_code, region
        FROM masterdatabase.farmer_personal_information
    """, engine)

    cfi = pd.read_sql("""
        SELECT contract_code, status
        FROM masterdatabase.contract_farmer_information
    """, engine)

    ca = pd.read_sql("""
        SELECT contract_code, usa_trees_planted, total_can_allocation
        FROM masterdatabase.contract_allocation
    """, engine)

    sc = pd.read_sql("""
        SELECT contract_code, current_surviving_trees
        FROM masterdatabase.survival_current
    """, engine)

    # ---- Enriquecer CTI con región (desde FPI)
    cti_en = cti.merge(fpi, on="contract_code", how="left")
    cti_en["region"] = cti_en["region"].astype("string").str.strip()
    cti_en["etp_year"] = pd.to_numeric(cti_en["etp_year"], errors="coerce").astype("Int64")
    cti_en["trees_contract"] = pd.to_numeric(cti_en["trees_contract"], errors="coerce")
    cti_en["planted"] = pd.to_numeric(cti_en["planted"], errors="coerce")

    # planting_year: prioridad planting_year -> planting_date.year -> etp_year
    py = pd.to_numeric(cti_en.get("planting_year"), errors="coerce").astype("Int64")
    if "planting_date" in cti_en.columns:
        dt = pd.to_datetime(cti_en["planting_date"], errors="coerce")
        py = py.fillna(dt.dt.year.astype("Int64"))
    py = py.fillna(cti_en["etp_year"])
    cti_en["planting_year"] = py.astype("Int64")

    # ---- Decide lado por año/país (para planted_calc)
    def side(row):
        alloc = get_allocation_type(int(row["etp_year"])) if pd.notna(row["etp_year"]) else []
        if alloc == ["ETP"]:
            return "US"
        if alloc == ["COP"]:
            return "CAN"
        if alloc == ["COP", "ETP"]:
            # Usa región desde FPI (ya viene en cti_en)
            return "US" if str(row.get("region", "")).strip().upper() == "USA" else "CAN"
        return None

    base_alloc = cti_en.merge(ca, on="contract_code", how="left")
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
    base_stats = (
        cti_en.merge(cfi, on="contract_code", how="left")
              .merge(sc,  on="contract_code", how="left")
    )
    base_stats = base_stats[base_stats["status"] == "Active"].copy()
    base_stats["current_surviving_trees"] = pd.to_numeric(
        base_stats["current_surviving_trees"], errors="coerce"
    ).fillna(0)

    base_stats["survival_pct"] = base_stats.apply(
        lambda r: (r["current_surviving_trees"] / r["trees_contract"])
        if pd.notna(r["trees_contract"]) and r["trees_contract"] > 0 else pd.NA,
        axis=1
    )

    # ---- Stats (NO ponderados) por planting_year
    stats_num, stats_txt = survival_stats(
        df=base_stats,
        group_col="planting_year",
        survival_pct_col="survival_pct"
    )
    # map rápido para resumen textual
    summary_map = dict(
        stats_txt.dropna(subset=["planting_year"])
                 .set_index("planting_year")["Survival_Summary"]
    )

    # ---- Agrega por planting_year x región (Planted / Surviving)
    planted_year_region = (
        base_alloc.groupby(["planting_year", "region"], dropna=False)["planted_calc"]
                  .sum(min_count=1)
                  .reset_index()
                  .rename(columns={"planted_calc": "Planted"})
    )

    surviving_year_region = (
        base_stats.groupby(["planting_year", "region"], dropna=False)["current_surviving_trees"]
                  .sum(min_count=1)
                  .reset_index()
                  .rename(columns={"current_surviving_trees": "Surviving"})
    )

    # Full grid
    df = pd.merge(planted_year_region, surviving_year_region,
                  on=["planting_year", "region"], how="outer").fillna(0)

    # Solo países que van en la tabla
    country_order = ["Costa Rica", "Guatemala", "Mexico", "USA"]
    df["region"] = df["region"].astype("string")
    df = df[df["region"].isin(country_order)]

    # ---- Larga: dos filas por año (Planted / Surviving)
    long_planted = (
        df.pivot_table(index="planting_year", columns="region", values="Planted", aggfunc="sum", fill_value=0)
          .reset_index()
    )
    long_surv = (
        df.pivot_table(index="planting_year", columns="region", values="Surviving", aggfunc="sum", fill_value=0)
          .reset_index()
    )

    # Asegura columnas y Totales
    for tbl in (long_planted, long_surv):
        for c in country_order:
            if c not in tbl.columns:
                tbl[c] = 0
    long_planted["Total"] = long_planted[country_order].sum(axis=1)
    long_surv["Total"]    = long_surv[country_order].sum(axis=1)

    # Survival % poblacional por cohorte
    rate = pd.merge(
        long_planted[["planting_year", "Total"]].rename(columns={"Total": "P"}),
        long_surv[["planting_year", "Total"]].rename(columns={"Total": "S"}),
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

        # Fila Planted (sin resumen)
        planted_row = [int(y), "Planted"] + [int(lp.get(c, 0)) for c in country_order] + [int(lp.get("Total", 0)), "", ""]
        # Fila Surviving (con % poblacional y Survival_Summary no ponderado)
        surviving_row = [int(y), "Surviving"] + [int(ls.get(c, 0)) for c in country_order] \
                        + [int(ls.get("Total", 0)), f"{round(surv_pct*100,1)}%", summary_txt]

        rows.extend([planted_row, surviving_row])

    cols = ["Year", "Row"] + country_order + ["Total", "Survival %", "Survival_Summary"]
    out = pd.DataFrame(rows, columns=cols)

    # Footer totales (opcional)
    total_plan = out[out["Row"] == "Planted"]["Total"].sum()
    total_surv = out[out["Row"] == "Surviving"]["Total"].sum()
    footer = pd.DataFrame([
        ["", "Total Planted"]   + [""] * len(country_order) + [int(total_plan), "", ""],
        ["", "Total Surviving"] + [""] * len(country_order) + [int(total_surv), "", ""],
    ], columns=cols)

    out = pd.concat([out, footer], ignore_index=True)
    return out
