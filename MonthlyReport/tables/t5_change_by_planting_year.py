# MasterDatabaseManagement/tables/t5_change_by_planting_year.py
# -*- coding: utf-8 -*-
from datetime import date
from core.libs import pd, np
from core.db import get_engine
from MonthlyReport.tables.t3_trees_by_planting_year import build_t3_trees_by_planting_year
from MonthlyReport.tables_process import align_to_template_headers
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

# ---------- MELTS ----------
def _melt_t3_base(df_t3: pd.DataFrame) -> pd.DataFrame:
    """
    T3 (wide) → long: (year_base, row_label, country, value_base)
    Acepta 'Year' o 'ETP Year' y 'Row' o 'Status of Trees'. Limita a {Planted, Surviving}.
    """
    if df_t3 is None or df_t3.empty:
        return pd.DataFrame(columns=["year_base","row_label","country","value_base"])

    base = df_t3.copy()

    # Columnas dinámicas
    col_year = "Year" if "Year" in base.columns else ("ETP Year" if "ETP Year" in base.columns else None)
    if col_year is None:
        raise KeyError("No encuentro columna de año en T3 (esperaba 'Year' o 'ETP Year').")

    col_row = "Row" if "Row" in base.columns else ("Status of Trees" if "Status of Trees" in base.columns else None)
    if col_row is None:
        raise KeyError("No encuentro columna de status en T3 (esperaba 'Row' o 'Status of Trees').")

    # Países (+ Total si existe)
    country_cols = [c for c in ALL_COUNTRIES_FOR_MELT if c in base.columns]
    need = [col_year, col_row, *country_cols]
    base = base[need].copy()

    long = base.melt(id_vars=[col_year, col_row], var_name="country", value_name="value_base")
    long["country"] = long["country"].replace({"Total": "TOTAL"})
    long = long.rename(columns={col_year: "year_base", col_row: "row_label"})
    long["year_base"] = pd.to_numeric(long["year_base"], errors="coerce")
    long["value_base"] = pd.to_numeric(long["value_base"], errors="coerce")

    # Solo métricas numéricas de interés
    long = long[long["row_label"].isin(["Planted","Surviving"])]
    return long

def _melt_t5_hist(df_hist: pd.DataFrame) -> pd.DataFrame:
    """
    T5 histórico (wide) → long: (year_base, row_label, country, value_hist)
    """
    if df_hist is None or df_hist.empty:
        return pd.DataFrame(columns=["year_base","row_label","country","value_hist"])

    keep = ["year_base", "row_label"] + [c for c in (COUNTRIES + ["TOTAL"]) if c in df_hist.columns]
    h = df_hist[keep].melt(id_vars=["year_base","row_label"], var_name="country", value_name="value_hist")
    h["year_base"] = pd.to_numeric(h["year_base"], errors="coerce")
    h["value_hist"] = pd.to_numeric(h["value_hist"], errors="coerce")
    h = h[h["row_label"].isin(["Planted","Surviving"])]
    return h

# ---------- UPSERT ----------
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
    VALUES (:change_month, :year_base, :metric, :country, :base_value, :hist_value, :diff_value)
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

        rows = df_upsert[["change_month","year_base","metric","country","base_value","hist_value","diff_value"]]
        payload = [_row_to_params(r) for _, r in rows.iterrows()]
        con.execute(stmt, payload)

# ---------- BUILDER ----------
def build_t5_change_by_planting_year(
    engine=None,
    run_month: str | date | None = None,
    mbt: pd.DataFrame | None = None,
    materialize: bool = False
) -> pd.DataFrame:
    """
    Diferencial T5 = T3 (calculado) - T5 histórico (último < corte).
    Devuelve long:
      columns: ["country","planting_year","Planted","Surviving"]
      valores = diferenciales por país y año.
    """
    engine = engine or get_engine()
    if isinstance(run_month, str):
        y, m = run_month.split("-")[0:2]
        run_month = date(int(y), int(m), 1)
    rm_curr, rm_hist = _resolve_months(engine, T5_HIST, run_month)

    # 1) Base T3
    if mbt is None:
        from MonthlyReport.utils_monthly_base import build_monthly_base_table
        mbt = build_monthly_base_table()
    t3 = build_t3_trees_by_planting_year(mbt)
    base_long = _melt_t3_base(t3)

    # 2) Histórico T5 (último < corte)
    hist_wide = pd.read_sql(
        f"SELECT * FROM {T5_HIST} WHERE run_month = %(rm)s",
        engine, params={"rm": rm_hist}
    ) if rm_hist is not None else pd.DataFrame()
    hist_long = _melt_t5_hist(hist_wide)

    # 3) Outer join + restas (NaN→0)
    j = base_long.merge(
        hist_long,
        on=["year_base","row_label","country"],
        how="outer",
        suffixes=("_base","_hist"),
    )
    j["value_base"] = pd.to_numeric(j["value_base"], errors="coerce").fillna(0)
    j["value_hist"] = pd.to_numeric(j["value_hist"], errors="coerce").fillna(0)
    j["diff"] = (j["value_base"] - j["value_hist"]).round(0)

    # 4) Pivot → columnas métricas
    piv = (
        j.pivot_table(index=["country","year_base"], columns="row_label", values="diff", aggfunc="sum", fill_value=0)
         .reset_index()
    )
    for col in ("Planted", "Surviving"):
        if col not in piv.columns:
            piv[col] = 0

    # Coerce year, ordenar y salida canon
    piv["year_base"] = pd.to_numeric(piv["year_base"], errors="coerce").astype("Int64")
    piv = piv.sort_values(["year_base","country"]).reset_index(drop=True)
    out = piv.rename(columns={"year_base":"planting_year"})[["country","planting_year","Planted","Surviving"]]

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

# ---------- FORMATTER (wide diff; TOTAL = suma países) ----------
def format_t5_matrix(df_long: pd.DataFrame, run_month: date | None = None) -> pd.DataFrame:
    """
    Genera 't5_wide' (diff) con el schema:
      run_month, year_base, row_label, Costa Rica, Guatemala, Mexico, USA, TOTAL,
      Survival, loaded_at, Series Obligation, Obligation Remaining
    """
    cols_schema = ["run_month","year_base","row_label", *COUNTRIES, "TOTAL",
                   "Survival","loaded_at","Series Obligation","Obligation Remaining"]

    if df_long is None or df_long.empty:
        run_month = run_month or _first_day_this_month()
        out = pd.DataFrame(columns=cols_schema)
        out["run_month"] = pd.to_datetime(run_month)
        return out[cols_schema]

    dfl = df_long[df_long["country"].notna() & (df_long["country"] != "TOTAL")].copy()
    # Orden fijo de países + otros al final si aparecieran
    col_order = [c for c in COUNTRIES if c in dfl["country"].unique().tolist()]
    other = [c for c in dfl["country"].unique().tolist() if c not in col_order and c != "TOTAL"]
    col_order = col_order + other

    rows = []
    for y, g in dfl.groupby("planting_year", sort=True):
        row_p = {"year_base": int(y), "row_label": "Planted"}
        row_s = {"year_base": int(y), "row_label": "Surviving"}

        for ctry in col_order:
            sub = g[g["country"] == ctry]
            row_p[ctry] = float(pd.to_numeric(sub["Planted"],  errors="coerce").sum())
            row_s[ctry] = float(pd.to_numeric(sub["Surviving"],errors="coerce").sum())

        row_p["TOTAL"] = sum(row_p.get(ctry, 0) for ctry in col_order)
        row_s["TOTAL"] = sum(row_s.get(ctry, 0) for ctry in col_order)

        # Survival (%) (si quieres mostrarlo; es diff, así que suele ser NaN)
        surv_pct = (row_s["TOTAL"] / row_p["TOTAL"] * 100.0) if row_p["TOTAL"] else np.nan
        row_p["Survival"] = np.nan
        row_s["Survival"] = round(float(surv_pct), 2) if pd.notna(surv_pct) else np.nan

        # Campos obligatorios
        row_p["Series Obligation"] = np.nan
        row_s["Series Obligation"] = np.nan
        row_p["Obligation Remaining"] = np.nan
        row_s["Obligation Remaining"] = np.nan

        rows += [row_p, row_s]

    out = pd.DataFrame(rows)

    # Tipado contable + orden
    for c in col_order + ["TOTAL"]:
        out[c] = pd.to_numeric(out[c], errors="coerce").round(0).astype("Int64")

    run_month = run_month or _first_day_this_month()
    out.insert(0, "run_month", pd.to_datetime(run_month))
    out["loaded_at"] = pd.Timestamp.now()

    # Asegura todas las columnas del schema
    for c in cols_schema:
        if c not in out.columns:
            out[c] = pd.NA

    out = out[cols_schema].sort_values(["year_base","row_label"]).reset_index(drop=True)

    # === Headers finales para T5 ===
    # 1) Renombrar claves
    out = out.rename(columns={
        "year_base": "Planting Year",
        "row_label": "Status of Trees",
    })

    # 2) Quitar columnas que no deben salir (tanto con espacios como en snake_case)
    drop_cols = [
        "run_month", "loaded_at",
        "Series Obligation", "series_obligation",
        "Obligation Remaining", "Obligation_Remaining",
    ]
    out = out.drop(columns=[c for c in drop_cols if c in out.columns], errors="ignore")

    # 3) Tipos limpios: países y TOTAL a int; deja métricas derivadas como float
    int_like = [c for c in ["TOTAL", "Costa Rica", "Guatemala", "Mexico", "USA"] if c in out.columns]
    if int_like:
        out[int_like] = (
            out[int_like]
            .apply(pd.to_numeric, errors="coerce")
            .fillna(0)
            .round(0)
            .astype(int)
        )

    # 4) Poner primero las claves
    first = [c for c in ["Planting Year", "Status of Trees"] if c in out.columns]
    rest = [c for c in out.columns if c not in first]
    out = out[first + rest]

    # (Opcional) orden final de filas por las nuevas claves
    out = out.sort_values(first).reset_index(drop=True)

    # === Alineación final para Excel (según template) ===
    # Renombres (por si acaso)
    rename_map_tpl = {
        "year_base": "Planting Year",
        "row_label": "Status of Trees",
        # Mantén TOTAL tal cual; si tu hoja dice "Total" cambia aquí:
        # "TOTAL": "Total",
    }

    # Orden exacto del template (según tu guía)
    template_cols_t5 = [
        "Planting Year", "Status of Trees",
        "Costa Rica", "Guatemala", "Mexico", "USA", "TOTAL",
        "Survival",
    ]

    # Aplicar alineación
    out = align_to_template_headers(out, template_cols_t5, rename_map=rename_map_tpl)

    # Ordenar filas por las claves principales
    out = out.sort_values(["Planting Year", "Status of Trees"]).reset_index(drop=True)

    return out

