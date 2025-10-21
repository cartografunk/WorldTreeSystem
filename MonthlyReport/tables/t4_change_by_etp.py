#MasterDatabaseManagement/tables/t4_change_by_etp.py
# -*- coding: utf-8 -*-
from datetime import date
from core.libs import pd, np
from core.db import get_engine
from MonthlyReport.tables.t2_trees_by_etp_raise import build_etp_trees_table2
from MonthlyReport.tables_process import align_to_template_headers

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
    NO filtra. Solo:
      - normaliza nombres
      - hace melt por país (incluye 'Total' mapeado a 'TOTAL')
      - arrastra 'Type of ETP' si existe
    """
    if df_t2 is None or df_t2.empty:
        return pd.DataFrame(columns=["year_base","row_label","country","value_base","type_of_etp"])

    base = df_t2.copy()

    # columnas estándar (sin filtrar)
    col_year = "ETP Year" if "ETP Year" in base.columns else "year"
    col_row  = "Status of Trees" if "Status of Trees" in base.columns else "contract_trees_status"
    col_typ  = "Type of ETP" if "Type of ETP" in base.columns else None

    # países + Total tal como venga T2
    country_cols = [c for c in ["Costa Rica","Guatemala","Mexico","USA","Total"] if c in base.columns]

    id_vars = [col_year, col_row] + ([col_typ] if col_typ else [])
    base = base[id_vars + country_cols].copy()

    long = base.melt(id_vars=id_vars, var_name="country", value_name="value_base")
    long["country"] = long["country"].replace({"Total": "TOTAL"})
    long = long.rename(columns={col_year: "year_base", col_row: "row_label"})
    long["value_base"] = pd.to_numeric(long["value_base"], errors="coerce")

    if col_typ:
        long = long.rename(columns={col_typ: "type_of_etp"})
    else:
        long["type_of_etp"] = pd.NA

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

def _apply_t4_headers(df):
    # 1) Renombrar
    rename_map = {
        "year_base": "Planting Year",
        "row_label": "Status of Trees",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # 2) Dropear
    drop_cols = ["run_month", "loaded_at"]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns], errors="ignore")

    # 3) Orden sugerido (si existen)
    preferred_first = ["Planting Year", "Status of Trees"]
    first = [c for c in preferred_first if c in df.columns]
    # deja el resto como están, respetando su orden actual
    rest = [c for c in df.columns if c not in first]
    df = df[first + rest]

    return df

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
    A partir del df_long de diffs que tiene columnas:
      country, etp_year, [Contracted, Planted, Surviving], (opcionales: series_obligation, Obligation_Remaining)
    """
    if df_long is None or df_long.empty:
        cols = ["run_month","year_base","row_label","Costa Rica","Guatemala","Mexico","USA","TOTAL",
                "Survival-based off of planted","Series Obligation",
                "Obligation Remaining (text)","Obligation Remaining (num)","loaded_at"]
        return pd.DataFrame(columns=cols)

    # Asegura columnas país
    present_countries = [c for c in COUNTRIES if c in df_long["country"].dropna().unique().tolist()]
    if not present_countries:
        present_countries = COUNTRIES

    # ---- Pivotes por estatus (sum by country) ----
    def _pivot_status(colname: str) -> pd.DataFrame:
        if colname not in df_long.columns:
            # columna no presente -> todo 0
            tmp = df_long[["etp_year","country"]].copy()
            tmp[colname] = 0.0
        else:
            tmp = df_long[["etp_year","country",colname]].copy()
        pv = tmp.pivot_table(index="etp_year", columns="country", values=colname,
                             aggfunc="sum", fill_value=0).reindex(columns=present_countries, fill_value=0)
        pv = pv.rename_axis(None, axis=1)  # quita el nombre del eje de columnas
        pv["TOTAL"] = pv.sum(axis=1)
        return pv

    pvC = _pivot_status("Contracted")
    pvP = _pivot_status("Planted")
    pvS = _pivot_status("Surviving")

    # ---- Survival-based off of planted (S / P) como fracción ----
    surv_frac = (pvS["TOTAL"] / pvP["TOTAL"]).replace([np.inf, -np.inf], np.nan)

    # ---- Series Obligation y Obligation Remaining ----
    # Si df_long trae series_obligation / Obligation_Remaining en filas TOTAL por año, úsalos.
    ser = df_long[df_long["country"] == "TOTAL"].drop_duplicates(subset=["etp_year"])
    ser_map = pd.Series(index=ser["etp_year"].values,
                        data=pd.to_numeric(ser.get("series_obligation", np.nan), errors="coerce")).to_dict()
    obr_map = pd.Series(index=ser["etp_year"].values,
                        data=pd.to_numeric(ser.get("Obligation_Remaining", np.nan), errors="coerce")).to_dict()

    # Construcción de filas
    rows = []
    for y in sorted(pvC.index.tolist()):
        # Helper para armar una fila por estatus
        def _row(label: str, src: pd.DataFrame):
            d = {"year_base": int(y), "row_label": label}
            for c in present_countries:
                d[c] = int(round(float(src.at[y, c]))) if c in src.columns else 0
            d["TOTAL"] = int(round(float(src.at[y, "TOTAL"]))) if "TOTAL" in src.columns else 0
            d["Survival-based off of planted"] = np.nan
            d["Series Obligation"] = np.nan
            d["Obligation Remaining (text)"] = np.nan
            d["Obligation Remaining (num)"] = np.nan
            return d

        # Contracted / Planted / Surviving
        row_c = _row("Contracted", pvC)
        row_p = _row("Planted",    pvP)
        row_s = _row("Surviving",  pvS)

        # Survival-based off of planted SOLO en la fila Surviving (fracción)
        frac = surv_frac.get(y, np.nan)
        row_s["Survival-based off of planted"] = float(round(frac, 2)) if pd.notna(frac) else np.nan

        # Series Obligation y Obligation Remaining:
        s_val = ser_map.get(y, np.nan)
        if pd.notna(s_val):
            row_s["Series Obligation"] = float(s_val)

        if y <= 2023:
            row_s["Obligation Remaining (text)"] = "Fulfilled"
            row_s["Obligation Remaining (num)"]  = 0
        else:
            if pd.notna(s_val):
                obr_num = s_val - row_c["TOTAL"]  # series − Contracted TOTAL (diferencial)
                row_s["Obligation Remaining (num)"] = int(round(max(0, obr_num)))
            # texto vacío en >2023 (como histórico)
            row_s["Obligation Remaining (text)"] = ""

        rows += [row_c, row_p, row_s]

    out = pd.DataFrame(rows)

    # Añade columnas faltantes para el schema
    # ====== Completar y tipar ======
    for c in COUNTRIES:
        if c not in out.columns:
            out[c] = 0
    if "TOTAL" not in out.columns:
        out["TOTAL"] = out[COUNTRIES].sum(axis=1)

    run_month = run_month or _first_day_this_month()
    out.insert(0, "run_month", pd.to_datetime(run_month))
    out["loaded_at"] = pd.Timestamp.now()

    # Orden base (DB-wide)
    cols_db = ["run_month", "year_base", "row_label", *COUNTRIES, "TOTAL",
               "Survival-based off of planted", "Series Obligation",
               "Obligation Remaining (text)", "Obligation Remaining (num)", "loaded_at"]
    out = out[cols_db].sort_values(["year_base", "row_label"]).reset_index(drop=True)

    # Tipos
    int_cols = [c for c in ["TOTAL", *COUNTRIES] if c in out.columns]
    out[int_cols] = out[int_cols].apply(pd.to_numeric, errors="coerce").fillna(0).round(0).astype(int)
    if "Survival-based off of planted" in out.columns:
        out["Survival-based off of planted"] = (
            pd.to_numeric(out["Survival-based off of planted"], errors="coerce").round(2)
        )

    # ====== Snapshot DB (no tocar headers DB) ======
    out_db = out.copy()  # si algún día quieres materializar t4_wide, usa este dataframe

    # ====== Versión Excel (headers del template) ======
    # Renombres para Excel (solo salida a Excel, NO DB)
    out_excel = out.rename(columns={
        "year_base": "Planting Year",
        "row_label": "Status of Trees",
        "Obligation Remaining (num)": "Obligation Remaining",
        # Mantén "TOTAL" tal cual para DB; el Excel pide "TOTAL" (según tu guía), así que no lo renombramos aquí.
        # Si tu template realmente usa "Total" (T mayúscula, resto minúsculas), cambia aquí:
        # "TOTAL": "Total",
    })

    # Orden exacto del template (sin run_month / loaded_at)
    from MonthlyReport.tables_process import align_to_template_headers
    template_cols_t4 = [
        "Planting Year", "Status of Trees",
        "Costa Rica", "Guatemala", "Mexico", "USA", "TOTAL",
        "Survival-based off of planted", "Series Obligation",
        "Obligation Remaining",
    ]
    out_excel = align_to_template_headers(out_excel, template_cols_t4)
    out_excel = out_excel.sort_values(["Planting Year", "Status of Trees"]).reset_index(drop=True)

    return out_excel
