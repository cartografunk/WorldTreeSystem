#MasterDatabaseManagement/tables/t4_change_by_etp.py
# -*- coding: utf-8 -*-
from datetime import date
from core.libs import pd, np
from core.db import get_engine
from MonthlyReport.tables.t2_trees_by_etp_raise import build_etp_trees_table2
from MonthlyReport.tables.t2a_trees_by_cop_raise import enrich_with_obligations_and_stats
from sqlalchemy import text as sqltext


# Tablas histórico y destino
T4_HIST   = "masterdatabase.t4_wide"
OUT_TABLE = "masterdatabase.t4_diff_from_t2"

COUNTRIES = ["Costa Rica", "Guatemala", "Mexico", "USA"]
ALL_COUNTRIES_FOR_MELT = COUNTRIES + ["Total"]  # T2 usa 'Total'; histórico usa 'TOTAL'

def _first_day_this_month() -> date:
    today = date.today()
    return date(today.year, today.month, 1)

def _resolve_months(engine, hist_table: str, run_month: date | None):
    """
    Devuelve:
      rm_curr = mes de reporte (si None -> primer día del mes actual)
      rm_hist = último run_month < rm_curr en el histórico
    """
    rm_curr = run_month or _first_day_this_month()
    rm_hist = pd.read_sql(
        f"SELECT MAX(run_month) AS m FROM {hist_table} WHERE run_month < %(rm)s",
        engine, params={"rm": rm_curr}
    )["m"].iloc[0]
    return rm_curr, rm_hist

def _melt_t2_base(df_t2: pd.DataFrame) -> pd.DataFrame:
    """
    T2 (nuevo schema) → long por (year_base=row_label=Status of Trees, country, value_base).
    Filtra Type of ETP == 'ETP'.
    """
    if df_t2.empty:
        return pd.DataFrame(columns=["year_base","row_label","country","value_base"])

    base = df_t2.copy()

    # Filtra ETP puro
    if "Type of ETP" in base.columns:
        base = base[base["Type of ETP"] == "ETP"].copy()

    # Normaliza nombres esperados
    col_year = "ETP Year" if "ETP Year" in base.columns else "year"
    col_row  = "Status of Trees" if "Status of Trees" in base.columns else "contract_trees_status"

    # Columnas país + Total (T2 trae 'Total', t4_wide trae 'TOTAL')
    country_cols = [c for c in ["Costa Rica","Guatemala","Mexico","USA","Total"] if c in base.columns]

    base = base[[col_year, col_row, *country_cols]].copy()
    long = base.melt(
        id_vars=[col_year, col_row],
        var_name="country", value_name="value_base"
    )
    long["country"] = long["country"].replace({"Total": "TOTAL"})
    long = long.rename(columns={col_year: "year_base", col_row: "row_label"})
    long["value_base"] = pd.to_numeric(long["value_base"], errors="coerce")
    return long


def _melt_t4_hist(df_hist: pd.DataFrame) -> pd.DataFrame:
    """
    T4 histórico (wide de DB) → long por año × status × país (incluye TOTAL).
    """
    if df_hist.empty:
        return pd.DataFrame(columns=["year_base","row_label","country","value_hist"])

    keep = ["year_base", "row_label"] + [c for c in (COUNTRIES + ["TOTAL"]) if c in df_hist.columns]
    h = df_hist[keep].melt(
        id_vars=["year_base", "row_label"],
        var_name="country", value_name="value_hist"
    )
    h["value_hist"] = pd.to_numeric(h["value_hist"], errors="coerce")
    return h

def _upsert_diff(engine, df_upsert: pd.DataFrame, out_table: str):
    if df_upsert.empty:
        return

    cols = ["change_month","year_base","metric","country","base_value","hist_value","diff_value"]

    ddl = f"""
    CREATE TABLE IF NOT EXISTS {out_table} (
      change_month DATE NOT NULL,
      year_base INT,
      metric TEXT,
      country TEXT,
      base_value NUMERIC,
      hist_value NUMERIC,
      diff_value NUMERIC,
      loaded_at TIMESTAMP NOT NULL DEFAULT NOW(),
      PRIMARY KEY (change_month, year_base, metric, country)
    );
    """

    insert_sql = f"""
    INSERT INTO {out_table} ({", ".join(cols)})
    VALUES (
      :change_month, :year_base, :metric, :country, :base_value, :hist_value, :diff_value
    )
    ON CONFLICT (change_month, year_base, metric, country) DO UPDATE
    SET base_value = EXCLUDED.base_value,
        hist_value = EXCLUDED.hist_value,
        diff_value = EXCLUDED.diff_value,
        loaded_at  = NOW();
    """

    with engine.begin() as con:
        # DDL en 2.0
        con.exec_driver_sql(ddl)

        stmt = sqltext(insert_sql)

        # Inserción batched con parámetros nombrados
        def _row_to_params(r):
            return {
                "change_month": r["change_month"],
                "year_base": None if pd.isna(r["year_base"]) else int(r["year_base"]),
                "metric": r["metric"],
                "country": r["country"],
                "base_value": None if pd.isna(r["base_value"]) else float(r["base_value"]),
                "hist_value": None if pd.isna(r["hist_value"]) else float(r["hist_value"]),
                "diff_value": None if pd.isna(r["diff_value"]) else float(r["diff_value"]),
            }

        batch = 3000  # ajusta si quieres
        rows = df_upsert[["change_month","year_base","metric","country","base_value","hist_value","diff_value"]]
        for i in range(0, len(rows), batch):
            payload = [ _row_to_params(r) for _, r in rows.iloc[i:i+batch].iterrows() ]
            con.execute(stmt, payload)

def build_t4_change_by_etp(engine=None, run_month: str | date | None = None,
                           mbt: pd.DataFrame | None = None,
                           so_by_year: dict | None = None,
                           materialize: bool = False) -> pd.DataFrame:
    engine = engine or get_engine()
    if isinstance(run_month, str):
        y, m = run_month.split("-")[0:2]; run_month = date(int(y), int(m), 1)
    rm_curr, rm_hist = _resolve_months(engine, T4_HIST, run_month)

    # 1) Base T2
    if mbt is None:
        from MonthlyReport.utils_monthly_base import build_monthly_base_table
        mbt = build_monthly_base_table()
    t2 = build_etp_trees_table2(mbt, so_by_year=so_by_year)

    base_long = _melt_t2_base(t2)

    # 2) Histórico T4 (último < corte) → long
    hist_wide = pd.read_sql(
        f"SELECT * FROM {T4_HIST} WHERE run_month = %(rm)s",
        engine, params={"rm": rm_hist}
    ) if rm_hist is not None else pd.DataFrame()
    hist_long = _melt_t4_hist(hist_wide)

    # 3) Join y resta por celda
    j = base_long.merge(hist_long, on=["year_base","row_label","country"], how="left")
    j["value_hist"] = j["value_hist"].fillna(0)
    j["diff"] = j["value_base"].fillna(0) - j["value_hist"]
    j["diff"] = j["diff"].round(0)  # <- fuerza enteros en el diferencial

    # --- Survival% diff (base% - hist%) por país y año_base ---
    gb = j.pivot_table(
        index=["country", "year_base"],
        columns="row_label",
        values=["value_base", "value_hist"],
        aggfunc="sum"
    ).fillna(0)
    gb.columns = [f"{a}__{b}" for a, b in gb.columns.to_flat_index()]
    gb = gb.reset_index()

    def _safe_pct(num, den):
        return (num / den * 100.0) if den and not pd.isna(den) and den != 0 else np.nan

    gb["survival_base_pct"] = gb.apply(lambda r: _safe_pct(r.get("value_base__Surviving", 0),
                                                           r.get("value_base__Planted", 0)), axis=1)
    gb["survival_hist_pct"] = gb.apply(lambda r: _safe_pct(r.get("value_hist__Surviving", 0),
                                                           r.get("value_hist__Planted", 0)), axis=1)
    gb["survival_pct_diff"] = gb["survival_base_pct"] - gb["survival_hist_pct"]

    # anexa survival_pct_diff al long (solo para TOTAL también agregaremos abajo en el format)
    surv_diff = gb.rename(columns={"year_base": "etp_year"})[["country", "etp_year", "survival_pct_diff"]]

    # 4) Pivot → columnas métricas (Contracted/Planted/Surviving)
    piv = (j.pivot_table(index=["country","year_base"], columns="row_label", values="diff", aggfunc="sum")
             .reset_index())

    piv = piv.rename(columns={"year_base":"etp_year"})
    out = piv[["country", "etp_year", "Contracted", "Planted", "Surviving"]].merge(
        surv_diff, on=["country", "etp_year"], how="left"
    )

    # 🔧 SOLO TOTAL a partir del join base-hist (j)
    df_total = j[j["country"] == "TOTAL"].copy()
    df_total = df_total.rename(columns={
        "year_base": "year",
        "row_label": "contract_trees_status"
    })
    df_total["Total"] = df_total["diff"]  # dummy para que enrich no truene

    # 1) Enriquecer para obtener Obligation_Remaining
    df_enriched = enrich_with_obligations_and_stats(df_total, engine)

    # 2) Traer series_obligation directo de la tabla origen
    # --- Series obligation por año
    series_tbl = pd.read_sql(
        "SELECT etp_year, series_obligation FROM masterdatabase.series_obligation",
        engine,
    )
    series_tbl["etp_year"] = pd.to_numeric(series_tbl["etp_year"], errors="coerce").astype("Int64")

    # --- Contracted Δ TOTAL por año (desde j)
    tot_contracted = (
        j[(j["country"] == "TOTAL") & (j["row_label"] == "Contracted")]
        .rename(columns={"year_base": "etp_year", "diff": "contracted_delta_total"})
        [["etp_year", "contracted_delta_total"]]
    )

    # Une a 'out' y calcula Obligation_Remaining Δ = Series − Contracted (TOTAL)
    out = out.merge(series_tbl, on="etp_year", how="left").merge(tot_contracted, on="etp_year", how="left")
    out["series_obligation"] = pd.to_numeric(out["series_obligation"], errors="coerce")
    out["contracted_delta_total"] = pd.to_numeric(out["contracted_delta_total"], errors="coerce").fillna(0)
    out["Obligation_Remaining"] = out["series_obligation"] - out["contracted_delta_total"]

    # 5) Materializar (opcional)
    if materialize:
        up = j.rename(columns={
            "row_label": "metric",
            "value_base": "base_value",
            "value_hist": "hist_value",
            "diff": "diff_value"
        }).copy()
        up["change_month"] = rm_curr
        _upsert_diff(engine, up, OUT_TABLE)

    return out


def format_t4_matrix(df_long: pd.DataFrame, run_month: date | None = None) -> pd.DataFrame:
    """
    Construye 't4_wide' (diff) con el schema histórico:
    run_month, year_base, row_label, Costa Rica, Guatemala, Mexico, USA, TOTAL,
    Survival-based off of planted, Series Obligation,
    Obligation Remaining (text), Obligation Remaining (num), loaded_at
    """
    if df_long.empty:
        cols = ["run_month","year_base","row_label","Costa Rica","Guatemala","Mexico","USA","TOTAL",
                "Survival-based off of planted","Series Obligation",
                "Obligation Remaining (text)","Obligation Remaining (num)","loaded_at"]
        return pd.DataFrame(columns=cols)

    # Países presentes
    present = df_long["country"].dropna().unique().tolist()
    col_order = [c for c in COUNTRIES if c in present]
    # ignora 'TOTAL' en este set; lo calcularemos nosotros

    # Pivotea del long de diffs a filas por (año, status)
    mat = []
    for y, g in df_long[df_long["country"].notna()].groupby("etp_year", sort=True):
        # agrupa por row_label -> diccionario de valores por país
        vals = {
            "Contracted": {c: 0.0 for c in col_order},
            "Planted":    {c: 0.0 for c in col_order},
            "Surviving":  {c: 0.0 for c in col_order},
        }
        for _, row in g.iterrows():
            lbl = row.get("row_label") or None
            if lbl not in vals:
                continue
            ctry = row["country"]
            if ctry in col_order:
                vals[lbl][ctry] += float(pd.to_numeric(row.get(lbl, 0), errors="coerce") or 0)

        # Totales por fila
        totals = {lbl: sum(vals[lbl].values()) for lbl in vals}

        # Survival-based off of planted (usar survival_pct_diff TOTAL si viene, si no calcular S/P)
        surv_pct = np.nan
        sub_tot = df_long[(df_long["country"] == "TOTAL") & (df_long["etp_year"] == y)]
        if not sub_tot.empty and "survival_pct_diff" in sub_tot.columns:
            v = pd.to_numeric(sub_tot["survival_pct_diff"].iloc[0], errors="coerce")
            surv_pct = float(v) if pd.notna(v) else np.nan
        else:
            p = totals.get("Planted", 0)
            s = totals.get("Surviving", 0)
            surv_pct = (s / p * 100.0) if p else np.nan

        # Obligations (usar columnas ya calculadas en df_long a nivel TOTAL)
        ser = np.nan
        obr_num = np.nan
        if not sub_tot.empty:
            if "series_obligation" in sub_tot.columns:
                ser = float(pd.to_numeric(sub_tot["series_obligation"].iloc[0], errors="coerce") or np.nan)
            if "Obligation_Remaining" in sub_tot.columns:
                obr_num = float(pd.to_numeric(sub_tot["Obligation_Remaining"].iloc[0], errors="coerce") or np.nan)

        # Fila Contracted
        row_c = {"year_base": int(y), "row_label": "Contracted"}
        row_c.update({c: int(vals["Contracted"][c]) for c in col_order})
        row_c["TOTAL"] = int(totals["Contracted"])
        row_c["Survival-based off of planted"] = np.nan
        row_c["Series Obligation"] = np.nan
        row_c["Obligation Remaining (text)"] = np.nan
        row_c["Obligation Remaining (num)"] = np.nan

        # Fila Planted
        row_p = {"year_base": int(y), "row_label": "Planted"}
        row_p.update({c: int(vals["Planted"][c]) for c in col_order})
        row_p["TOTAL"] = int(totals["Planted"])
        row_p["Survival-based off of planted"] = np.nan
        row_p["Series Obligation"] = np.nan
        row_p["Obligation Remaining (text)"] = np.nan
        row_p["Obligation Remaining (num)"] = np.nan

        # Fila Surviving
        row_s = {"year_base": int(y), "row_label": "Surviving"}
        row_s.update({c: int(vals["Surviving"][c]) for c in col_order})
        row_s["TOTAL"] = int(totals["Surviving"])
        row_s["Survival-based off of planted"] = round(surv_pct/100.0, 2) if pd.notna(surv_pct) else np.nan  # ❗ fracción como tu histórico
        row_s["Series Obligation"] = ser
        # Texto de obligación: "Fulfilled" hasta 2023
        row_s["Obligation Remaining (text)"] = "Fulfilled" if y <= 2023 else ""
        # Numérico: si Fulfilled => 0, si no => (series - Contracted_TOTAL) clamp ≥ 0
        if y <= 2023:
            row_s["Obligation Remaining (num)"] = 0
        else:
            if pd.notna(ser):
                obr_calc = ser - totals["Contracted"]
                row_s["Obligation Remaining (num)"] = max(0, int(round(obr_calc))) if pd.notna(obr_calc) else np.nan
            else:
                row_s["Obligation Remaining (num)"] = np.nan

        mat += [row_c, row_p, row_s]

    # DataFrame final y columnas en orden
    out = pd.DataFrame(mat)
    for c in COUNTRIES:
        if c not in out.columns: out[c] = 0
    if "TOTAL" not in out.columns: out["TOTAL"] = out[COUNTRIES].sum(axis=1)

    # run_month / loaded_at
    run_month = run_month or _first_day_this_month()
    out.insert(0, "run_month", pd.to_datetime(run_month))
    out["loaded_at"] = pd.Timestamp.now()

    cols = ["run_month","year_base","row_label",*COUNTRIES,"TOTAL",
            "Survival-based off of planted","Series Obligation",
            "Obligation Remaining (text)","Obligation Remaining (num)","loaded_at"]
    out = out[cols].sort_values(["year_base","row_label"]).reset_index(drop=True)
    return out

