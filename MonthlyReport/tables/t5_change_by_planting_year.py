#MasterDatabaseManagement/tables/t5_change_by_planting_year.py
# -*- coding: utf-8 -*-
from datetime import date
from core.libs import pd, np
from core.db import get_engine
from MonthlyReport.tables.t3_trees_by_planting_year import build_t3_trees_by_planting_year
from sqlalchemy import text as sqltext

# Tablas histórico y destino
T5_HIST   = "masterdatabase.t5_wide"
OUT_TABLE = "masterdatabase.t5_diff_from_t3"

COUNTRIES = ["Costa Rica", "Guatemala", "Mexico", "USA"]
ALL_COUNTRIES_FOR_MELT = COUNTRIES + ["Total"]  # T3 usa 'Total'; histórico usa 'TOTAL'

def _first_day_this_month() -> date:
    today = date.today()
    return date(today.year, today.month, 1)

def _resolve_months(engine, hist_table: str, run_month: date | None):
    rm_curr = run_month or _first_day_this_month()
    rm_hist = pd.read_sql(
        f"SELECT MAX(run_month) AS m FROM {hist_table} WHERE run_month < %(rm)s",
        engine, params={"rm": rm_curr}
    )["m"].iloc[0]
    return rm_curr, rm_hist

def _melt_t3_base(df_t3: pd.DataFrame) -> pd.DataFrame:
    """
    T3 base (calculado) → long por año × Row × país (Planted/Surviving).
    """
    if df_t3.empty:
        return df_t3

    need = ["Year", "Row"] + [c for c in ALL_COUNTRIES_FOR_MELT if c in df_t3.columns]
    base = df_t3[need].copy()

    long = base.melt(
        id_vars=["Year", "Row"],
        var_name="country", value_name="value_base"
    )
    long["country"] = long["country"].replace({"Total": "TOTAL"})
    long = long.rename(columns={"Year": "year_base", "Row": "row_label"})
    long["value_base"] = pd.to_numeric(long["value_base"], errors="coerce")
    # Solo métricas numéricas que nos interesan
    long = long[long["row_label"].isin(["Planted","Surviving"])]
    return long

def _melt_t5_hist(df_hist: pd.DataFrame) -> pd.DataFrame:
    if df_hist.empty:
        return pd.DataFrame(columns=["year_base","row_label","country","value_hist"])
    keep = ["year_base", "row_label"] + [c for c in (COUNTRIES + ["TOTAL"]) if c in df_hist.columns]
    h = df_hist[keep].melt(
        id_vars=["year_base", "row_label"],
        var_name="country", value_name="value_hist"
    )
    h["value_hist"] = pd.to_numeric(h["value_hist"], errors="coerce")
    # Solo Planted/Surviving
    h = h[h["row_label"].isin(["Planted","Surviving"])]
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
        con.exec_driver_sql(ddl)

        stmt = sqltext(insert_sql)

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

        batch = 3000
        rows = df_upsert[["change_month","year_base","metric","country","base_value","hist_value","diff_value"]]
        for i in range(0, len(rows), batch):
            payload = [ _row_to_params(r) for _, r in rows.iloc[i:i+batch].iterrows() ]
            con.execute(stmt, payload)

def build_t5_change_by_planting_year(engine=None, run_month: str | date | None = None, materialize: bool = False) -> pd.DataFrame:
    """
    Diferencial T5 = T3 (calculado) - T5 histórico (último < corte).
    - run_month: 'YYYY-MM'/'YYYY-MM-DD' o date; si None usa mes actual.
    - Devuelve long (por país): country, planting_year, planted, surviving (valores = diferencial).
    - Si materialize=True: upsert a masterdatabase.t5_diff_from_t3 con change_month = corte.
    """
    engine = engine or get_engine()
    if isinstance(run_month, str):
        y, m = run_month.split("-")[0:2]; run_month = date(int(y), int(m), 1)
    rm_curr, rm_hist = _resolve_months(engine, T5_HIST, run_month)

    # 1) Base T3 (calculado)
    t3 = build_t3_trees_by_planting_year(engine)  # columnas: Year, Row, países, Total, ...
    base_long = _melt_t3_base(t3)

    # 2) Histórico T5 (último < corte) → long
    hist_wide = pd.read_sql(
        f"SELECT * FROM {T5_HIST} WHERE run_month = %(rm)s",
        engine, params={"rm": rm_hist}
    ) if rm_hist is not None else pd.DataFrame()
    hist_long = _melt_t5_hist(hist_wide)

    # 3) Join y resta por celda
    j = base_long.merge(hist_long, on=["year_base","row_label","country"], how="left")
    j["value_hist"] = j["value_hist"].fillna(0)
    j["diff"] = j["value_base"].fillna(0) - j["value_hist"]
    j["diff"] = j["diff"].round(0)  # <- fuerza enteros en el diferencial

    # 4) Pivot → columnas métricas
    piv = (j.pivot_table(index=["country","year_base"], columns="row_label", values="diff", aggfunc="sum")
             .reset_index())
    piv.columns = [c.lower() if c in ["Planted","Surviving"] else c for c in piv.columns]
    for col in ("planted", "surviving"):
        if col not in piv.columns: piv[col] = 0
        piv[col] = pd.to_numeric(piv[col], errors="coerce").round(0).astype("Int64")

    piv = piv.rename(columns={"year_base": "etp_year"})
    out = piv[["country", "etp_year", "planted", "surviving"]]

    # 5) Materializar (opcional)
    if materialize:
        up = j.rename(columns={
            "row_label":"metric",
            "value_base":"base_value",
            "value_hist":"hist_value",
            "diff":"diff_value"
        }).copy()
        up["change_month"] = rm_curr
        _upsert_diff(engine, up, OUT_TABLE)

    return out


def format_t5_matrix(df_long: pd.DataFrame) -> pd.DataFrame:
    """
    Matriz tipo Excel (diferencial):
      - Columnas = países
      - Filas = etp_year × (Planted/Surviving)
      - TOTAL = suma por fila.
      - Survival (columna): (Surviving / Planted) * 100 (solo se rellena en la fila "Surviving").
    """
    if df_long.empty:
        return df_long

    dfl = df_long[df_long["country"].notna() & (df_long["country"] != "TOTAL")].copy()
    col_order = [c for c in COUNTRIES if c in dfl["country"].unique().tolist()]
    other = [c for c in dfl["country"].unique().tolist() if c not in col_order and c != "TOTAL"]
    col_order = col_order + other

    rows = []
    for y, g in dfl.groupby("etp_year", sort=True):
        row_p = {"etp_year": y, "contract_trees_status": "Planted"}
        row_s = {"etp_year": y, "contract_trees_status": "Surviving"}
        for ctry in col_order:
            sub = g[g["country"] == ctry]
            row_p[ctry] = float(pd.to_numeric(sub["planted"],   errors="coerce").sum())
            row_s[ctry] = float(pd.to_numeric(sub["surviving"], errors="coerce").sum())
        row_p["TOTAL"] = sum(row_p.get(ctry, 0) for ctry in col_order)
        row_s["TOTAL"] = sum(row_s.get(ctry, 0) for ctry in col_order)

        # Survival (solo en fila Surviving)
        surv_row_total = (row_s["TOTAL"] / row_p["TOTAL"] * 100.0) if row_p["TOTAL"] not in (0, np.nan) and not pd.isna(row_p["TOTAL"]) else np.nan
        row_p["Survival"] = np.nan
        row_s["Survival"] = surv_row_total

        rows += [row_p, row_s]

    out = pd.DataFrame(rows)
    cols = ["etp_year", "contract_trees_status"] + col_order + ["TOTAL", "Survival"]
    for c in col_order + ["TOTAL"]:
        out[c] = pd.to_numeric(out[c], errors="coerce").round(0).astype("Int64")
    out = out[cols]

    # 🔴 Renombrar Survival -> Survival (%)
    out = out.rename(columns={
        "etp_year": "ETP Year",
        "contract_trees_status": "Status of Trees",
        "Survival": "Survival (%)"
    })

    return out

