#MasterDatabaseManagement/tables/t4_change_by_etp.py
# -*- coding: utf-8 -*-
from datetime import date
from core.libs import pd, np
from core.db import get_engine
from MonthlyReport.tables.t2_trees_by_etp_raise import build_etp_trees_table2
from MonthlyReport.tables.t2a_trees_by_etp_stats_obligation import enrich_with_obligations_and_stats
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
    T2 base (calculado) → long por año × status × país.
    Usamos solo etp='ETP' (US ETP Raise).
    """
    if df_t2.empty:
        return df_t2

    base = df_t2.copy()
    # Nos quedamos con filas ETP (coincide con T4 "Trees by US ETP Raise")
    if "etp" in base.columns:
        base = base[base["etp"] == "ETP"].copy()

    # year / contract_trees_status / países + Total
    need = ["year", "contract_trees_status"] + [c for c in ALL_COUNTRIES_FOR_MELT if c in base.columns]
    base = base[need].copy()

    long = base.melt(
        id_vars=["year", "contract_trees_status"],
        var_name="country", value_name="value_base"
    )
    # Normaliza Total → TOTAL para empatar con histórico
    long["country"] = long["country"].replace({"Total": "TOTAL"})
    long = long.rename(columns={"year": "year_base", "contract_trees_status": "row_label"})
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

def build_t4_change_by_etp(engine=None, run_month: str | date | None = None, materialize: bool = False) -> pd.DataFrame:
    """
    Diferencial T4 = T2 (calculado) - T4 histórico(último < corte).
    - run_month: 'YYYY-MM'/'YYYY-MM-DD' o date; si None usa mes actual.
    - Devuelve long (por país): country, etp_year, contracted, planted, surviving (valores = diferencial).
    - Si materialize=True: upsert a masterdatabase.t4_diff_from_t2 con change_month = corte.
    """
    engine = engine or get_engine()
    if isinstance(run_month, str):
        y, m = run_month.split("-")[0:2]; run_month = date(int(y), int(m), 1)
    rm_curr, rm_hist = _resolve_months(engine, T4_HIST, run_month)

    # 1) Base T2 (calculado)
    t2 = build_etp_trees_table2(engine)  # columnas: year, etp, contract_trees_status, países, Total
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
    series_tbl = pd.read_sql(
        "SELECT etp_year, series_obligation FROM masterdatabase.series_obligation",
        engine,
    )
    series_tbl["etp_year"] = pd.to_numeric(series_tbl["etp_year"], errors="coerce").astype("Int64")

    # 3) De df_enriched solo pedimos Obligation_Remaining (porque series_obligation ya no está)
    obligations = df_enriched[["year", "contract_trees_status", "Obligation_Remaining"]].copy()
    obligations = obligations.rename(columns={"year": "etp_year"})

    # 4) Unimos series_obligation y Obligation_Remaining al out
    out = out.merge(series_tbl, on="etp_year", how="left")

    # --- Calcular Obligation Remaining Δ = Series Δ - Contracted Δ
    out["series_obligation"] = pd.to_numeric(out["series_obligation"], errors="coerce")
    out["Contracted"] = pd.to_numeric(out["Contracted"], errors="coerce")
    out["Obligation_Remaining"] = out["series_obligation"] - out["Contracted"]

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


def format_t4_matrix(df_long: pd.DataFrame) -> pd.DataFrame:
    """
    Matriz tipo Excel (diferencial):
      - Columnas = países
      - Filas = etp_year × (Contracted/Planted/Surviving)
      - TOTAL = suma por fila.
      - Survival-based off of planted = (Surviving / Planted) * 100 (solo se rellena en la fila "Surviving")
      - Series Obligation / Obligation Remaining: columnas placeholder (NaN) para compatibilidad con schema.
    """
    if df_long.empty:
        return df_long

    dfl = df_long[df_long["country"].notna() & (df_long["country"] != "TOTAL")].copy()
    col_order = [c for c in COUNTRIES if c in dfl["country"].unique().tolist()]
    other = [c for c in dfl["country"].unique().tolist() if c not in col_order and c != "TOTAL"]
    col_order = col_order + other

    rows = []
    for y, g in dfl.groupby("etp_year", sort=True):
        row_c = {"year": y, "contract_trees_status": "Contracted"}
        row_p = {"year": y, "contract_trees_status": "Planted"}
        row_s = {"year": y, "contract_trees_status": "Surviving"}
        for ctry in col_order:
            sub = g[g["country"] == ctry]
            row_c[ctry] = float(pd.to_numeric(sub["Contracted"], errors="coerce").sum())
            row_p[ctry] = float(pd.to_numeric(sub["Planted"],    errors="coerce").sum())
            row_s[ctry] = float(pd.to_numeric(sub["Surviving"],  errors="coerce").sum())
        row_c["TOTAL"] = sum(row_c.get(ctry, 0) for ctry in col_order)
        row_p["TOTAL"] = sum(row_p.get(ctry, 0) for ctry in col_order)
        row_s["TOTAL"] = sum(row_s.get(ctry, 0) for ctry in col_order)

        # Survival-based off of planted (solo en fila Surviving)
        surv_row = {}
        for ctry in col_order + ["TOTAL"]:
            p = row_p.get(ctry, 0)
            s = row_s.get(ctry, 0)
            surv_row[ctry] = (s / p * 100.0) if (p not in (0, np.nan) and not pd.isna(p)) else np.nan
        # Placeholders de obligaciones
        row_c["Survival-based off of planted"] = np.nan
        row_p["Survival-based off of planted"] = np.nan
        row_s["Survival-based off of planted"] = np.nan
        row_c["Series Obligation"] = np.nan
        row_p["Series Obligation"] = np.nan
        row_s["Series Obligation"] = np.nan
        row_c["Obligation Remaining"] = np.nan
        row_p["Obligation Remaining"] = np.nan
        row_s["Obligation Remaining"] = np.nan

        # Survival (%) desde df_long (TOTAL del año), como ya lo haces
        surv_pct_total = np.nan
        if "survival_pct_diff" in df_long.columns:
            sub = df_long[(df_long["country"] == "TOTAL") & (df_long["etp_year"] == y)]
            if not sub.empty and not pd.isna(sub["survival_pct_diff"].iloc[0]):
                surv_pct_total = float(sub["survival_pct_diff"].iloc[0])

        # 👉 Mapas para obligaciones (si vienen del builder)
        # Obligations: valores solo desde TOTAL del mismo año
        series_val, oblig_rem = np.nan, np.nan
        sub_tot = df_long[(df_long["country"] == "TOTAL") & (df_long["etp_year"] == y)]
        if not sub_tot.empty:
            if "series_obligation" in sub_tot.columns:
                v = sub_tot["series_obligation"].iloc[0]
                series_val = float(v) if pd.notna(v) else np.nan
            if "Obligation_Remaining" in sub_tot.columns:
                v = sub_tot["Obligation_Remaining"].iloc[0]
                oblig_rem = float(v) if pd.notna(v) else np.nan

        row_c["Series Obligation"] = np.nan
        row_p["Series Obligation"] = np.nan
        row_s["Series Obligation"] = series_val

        row_c["Obligation Remaining"] = np.nan
        row_p["Obligation Remaining"] = np.nan
        row_s["Obligation Remaining"] = oblig_rem

        rows += [row_c, row_p, row_s]

        # Rellenar Survival-based off of planted en la fila Surviving
        for ctry in col_order + ["TOTAL"]:
            row_s["Survival-based off of planted"] = np.nan  # keep single value per row (schema column)
        # Para mantener una sola celda por fila, dejamos el valor a nivel fila (no por país); usamos TOTAL por consistencia
        row_s["Survival-based off of planted"] = surv_row.get("TOTAL", np.nan)

        rows += [row_c, row_p, row_s]

    out = pd.DataFrame(rows)

    # 🔧 Asegura columnas obligatorias aunque vengan vacías
    for must in ["Survival-based off of planted", "Series Obligation", "Obligation Remaining"]:
        if must not in out.columns:
            out[must] = np.nan

    # 🔧 Si calculas el ∆ Survival (%) en este formatter, ponlo aquí:
    #   - Si ya traes survival_pct_diff en df_long (TOTAL por año), úsalo
    #   - Si no, deja NaN (o calcula fallback con deltas)
    if "Survival-based off of planted" in out.columns:
        # rellena SOLO filas Surviving con el valor del año correspondiente
        if "survival_pct_diff" in df_long.columns:
            # mapea: year -> survival_pct_diff (TOTAL)
            m = (df_long["country"] == "TOTAL")
            surv_map = df_long.loc[m, ["etp_year", "survival_pct_diff"]].dropna()
            surv_map = dict(zip(surv_map["etp_year"], surv_map["survival_pct_diff"]))
            out.loc[out["contract_trees_status"] == "Surviving", "Survival-based off of planted"] = \
                out.loc[out["contract_trees_status"] == "Surviving", "year"].map(surv_map)

    # 👉 Orden final (usa el nombre viejo para evitar KeyError en el slice)
    cols = ["year", "contract_trees_status"] + col_order + [
        "TOTAL", "Survival-based off of planted", "Series Obligation", "Obligation Remaining"
    ]

    for c in col_order + ["TOTAL"]:
        out[c] = pd.to_numeric(out[c], errors="coerce").round(0).astype("Int64")

    out = out[cols]

    # Calcular Obligation_Remaining Δ = Series Δ - Contracted Δ
    if "series_obligation" in out.columns and "Contracted" in out.columns:
        out["Obligation_Remaining"] = out["series_obligation"] - out["Contracted"]

    out = out.rename(columns={
        "year": "ETP Year",
        "contract_trees_status": "Status of Trees",
        "Survival-based off of planted": "Survival (%)"
    })

    return out
