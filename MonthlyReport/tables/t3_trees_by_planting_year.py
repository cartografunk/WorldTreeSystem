# MonthlyReport/tables/t3_trees_by_planting_year.py
from core.libs import pd
from MonthlyReport.tables_process import get_allocation_type

def build_t3_trees_by_planting_year(engine):
    # ---- Bases
    cti = pd.read_sql("""
        SELECT contract_code, etp_year, region, planted,
               planting_year, planting_date
        FROM masterdatabase.contract_tree_information
    """, engine)

    ca = pd.read_sql("""
        SELECT contract_code, usa_trees_planted, total_can_allocation
        FROM masterdatabase.contract_allocation
    """, engine)

    sc = pd.read_sql("""
        SELECT sc.contract_code, sc.current_surviving_trees
        FROM masterdatabase.survival_current sc
    """, engine)

    # ---- Normaliza
    cti["region"] = cti["region"].astype("string").str.strip()
    cti["etp_year"] = pd.to_numeric(cti["etp_year"], errors="coerce").astype("Int64")

    # planting_year: prioridad planting_year -> planting_date.year -> etp_year
    py = pd.to_numeric(cti.get("planting_year"), errors="coerce").astype("Int64")
    if "planting_date" in cti.columns:
        dt = pd.to_datetime(cti["planting_date"], errors="coerce")
        py = py.fillna(dt.dt.year.astype("Int64"))
    py = py.fillna(cti["etp_year"])
    cti["planting_year"] = py.astype("Int64")

    # ---- Une allocation para distinguir qué contar como planted por lado
    base = cti.merge(ca, on="contract_code", how="left")

    # Decide lado por año (ETP=USA, COP=CAN, mixto: heurística por region)
    def side(row):
        alloc = get_allocation_type(int(row["etp_year"])) if pd.notna(row["etp_year"]) else []
        if alloc == ["ETP"]:
            return "US"
        if alloc == ["COP"]:
            return "CAN"
        if alloc == ["COP", "ETP"]:
            return "US" if str(row.get("region","")).strip().upper() == "USA" else "CAN"
        return None
    base["side"] = base.apply(side, axis=1)

    # Planted por país (si lado=US usar usa_trees_planted o fallback a cti.planted; si CAN usar total_can_allocation)
    def planted_val(r):
        if r["side"] == "US":
            return r["usa_trees_planted"] if pd.notna(r["usa_trees_planted"]) else r["planted"]
        if r["side"] == "CAN":
            return r["total_can_allocation"]
        return None
    base["planted_calc"] = pd.to_numeric(base.apply(planted_val, axis=1), errors="coerce").fillna(0)

    # ---- Surviving: asocia supervivencia al cohort del planting_year
    surv = sc.merge(cti[["contract_code","planting_year","region"]], on="contract_code", how="left")
    surv["planting_year"] = pd.to_numeric(surv["planting_year"], errors="coerce").astype("Int64")
    surv["region"] = surv["region"].astype("string").str.strip()

    # ---- Agrega por planting_year x región
    planted_year_region = (base
        .groupby(["planting_year","region"], dropna=False)["planted_calc"]
        .sum(min_count=1).reset_index().rename(columns={"planted_calc":"Planted"}))

    surviving_year_region = (surv
        .groupby(["planting_year","region"], dropna=False)["current_surviving_trees"]
        .sum(min_count=1).reset_index().rename(columns={"current_surviving_trees":"Surviving"}))

    # Full grid
    df = pd.merge(planted_year_region, surviving_year_region,
                  on=["planting_year","region"], how="outer").fillna(0)

    # Solo países que van en la tabla
    country_order = ["Costa Rica","Guatemala","Mexico","USA"]
    df["region"] = df["region"].astype("string")
    df = df[df["region"].isin(country_order)]

    # ---- Larga: dos filas por año
    long_planted = df.pivot_table(index="planting_year", columns="region", values="Planted", aggfunc="sum", fill_value=0).reset_index()
    long_surv    = df.pivot_table(index="planting_year", columns="region", values="Surviving", aggfunc="sum", fill_value=0).reset_index()

    # Ordena columnas de países
    for tbl in (long_planted, long_surv):
        for c in country_order:
            if c not in tbl.columns:
                tbl[c] = 0
        tbl = tbl  # (mantener referencia)

    # Totales por fila
    long_planted["TOTAL"] = long_planted[country_order].sum(axis=1)
    long_surv["TOTAL"]    = long_surv[country_order].sum(axis=1)

    # Survival % por cohorte (surv / planted)
    rate = pd.merge(long_planted[["planting_year","TOTAL"]].rename(columns={"TOTAL":"P"}),
                    long_surv[["planting_year","TOTAL"]].rename(columns={"TOTAL":"S"}),
                    on="planting_year", how="outer").fillna(0)
    rate["Survival %"] = (rate["S"] / rate["P"]).where(rate["P"] > 0, 0)

    # ---- Concat estilo final (dos filas por año)
    rows = []
    idx_lp = long_planted.set_index("planting_year")
    idx_ls = long_surv.set_index("planting_year")
    idx_rt = rate.set_index("planting_year")

    for y in sorted(set(idx_lp.index).union(idx_ls.index)):
        lp = idx_lp.loc[y] if y in idx_lp.index else pd.Series({c: 0 for c in country_order} | {"TOTAL": 0})
        ls = idx_ls.loc[y] if y in idx_ls.index else pd.Series({c: 0 for c in country_order} | {"TOTAL": 0})
        surv_pct = float(idx_rt.loc[y, "Survival %"]) if y in idx_rt.index else 0.0

        planted_row = [int(y), "Planted"] + [int(lp.get(c, 0)) for c in country_order] + [int(lp.get("TOTAL", 0)), ""]
        surviving_row = [int(y), "Surviving"] + [int(ls.get(c, 0)) for c in country_order] + [int(ls.get("TOTAL", 0)),
                                                                                              f"{round(surv_pct * 100):.0f}%"]

        rows.extend([planted_row, surviving_row])

    cols = ["Year","Row"] + country_order + ["TOTAL","Survival"]
    out = pd.DataFrame(rows, columns=cols)

    # Totales del bloque inferior
    total_plan = out[out["Row"]=="Planted"]["TOTAL"].sum()
    total_surv = out[out["Row"]=="Surviving"]["TOTAL"].sum()
    footer = pd.DataFrame([
        ["", "Total Planted"] + [""]*len(country_order) + [int(total_plan), ""],
        ["", "Total Surviving"] + [""]*len(country_order) + [int(total_surv), ""],
    ], columns=cols)

    out = pd.concat([out, footer], ignore_index=True)
    return out
