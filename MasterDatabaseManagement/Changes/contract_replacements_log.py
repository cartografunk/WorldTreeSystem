# -*- coding: utf-8 -*-
"""
ContractReplacementsLog → masterdatabase.contract_replacements_ts

Hoja de entrada en changelog.xlsx:
  ContractReplacementsLog(requester, date, contract_code, trees_replaced, Species/Strain, note, change_in_db)

Transformación:
  - year = YEAR(date)
  - replaced_count = SUM(trees_replaced) por (contract_code, year) con change_in_db == 'Ready'
  - pct_replaced = replaced_count / trees_contract del mismo (contract_code, year) usando planting_year,
                   si no hay planting_year, intentar etp_year; si no hay denominador, pct_replaced = NULL
  - loaded_at = NOW() (en la inserción)

Requiere:
  - core.sheets.Sheet
  - core.paths.DATABASE_EXPORTS_DIR
  - core.db.get_engine
"""
from __future__ import annotations

from core.libs import pd, np, text
from core.db import get_engine
from core.paths import DATABASE_EXPORTS_DIR
from core.sheets import Sheet, STATUS_DONE
from pathlib import Path
from typing import Tuple, Dict

CATALOG_FILE = Path(DATABASE_EXPORTS_DIR) / "changelog.xlsx"
SHEET_NAME   = "ContractReplacementsLog"

HEADERS = [
    "requester", "date", "contract_code", "trees_replaced",
    "Species/Strain",  # 👈 nuevo encabezado
    "note", "change_in_db"
]

UPSERT_SQL = text("""
INSERT INTO masterdatabase.contract_replacements_ts AS t
    (contract_code, year, replaced_count, pct_replaced, species_strain, loaded_at)
VALUES
    (:contract_code, :year, :replaced_count, :pct_replaced, :species_strain, NOW())
ON CONFLICT (contract_code, year) DO UPDATE
    SET replaced_count = EXCLUDED.replaced_count,
        pct_replaced  = EXCLUDED.pct_replaced,
        species_strain= EXCLUDED.species_strain,
        loaded_at     = NOW()
""")


def ensure_contract_replacements_sheet() -> None:
    """
    Crea la hoja ContractReplacementsLog en changelog.xlsx si no existe.
    No sobreescribe si ya existe. Deja solo encabezados.
    """
    sheet = Sheet(CATALOG_FILE, SHEET_NAME)
    if sheet.ws is None:
        # crea y escribe headers
        sheet.create()
        for c, h in enumerate(HEADERS, start=1):
            sheet.ws.cell(row=1, column=c, value=h)
        sheet.save()
    else:
        # asegura que estén los encabezados (sin pisar datos)
        missing = [h for h in HEADERS if h not in sheet.headers]
        if missing:
            # añade columnas faltantes al final
            for h in missing:
                sheet.append_column(h)
            sheet.save()

def _pick_species_col(df: pd.DataFrame) -> str | None:
    for c in ["Species/Strain", "Species", "species_strain", "species", "Species_strain"]:
        if c in df.columns:
            return c
    return None

def _read_ready_entries() -> pd.DataFrame:
    sheet = Sheet(CATALOG_FILE, SHEET_NAME)
    df = sheet.to_dataframe()
    if df.empty:
        return df

    for col in HEADERS:
        if col not in df.columns:
            df[col] = np.nan

    # Filtra Ready (sin tocar el texto de species)
    mask_ready = df["change_in_db"] == "Ready"
    df = df[mask_ready].copy()

    # Campos mínimos (estos sí deben parsearse)
    df["contract_code"] = df["contract_code"].replace({"": np.nan})
    df["trees_replaced"] = pd.to_numeric(df["trees_replaced"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)
    df["year"] = df["date"].dt.year

    # Species passthrough (sin normalizar)
    sp_col = _pick_species_col(df)
    if sp_col is None:
        df["species_strain"] = np.nan
    else:
        # Mantener EXACTO lo escrito por el usuario (incluyendo espacios)
        df["species_strain"] = df[sp_col]

    ok = (
        df["contract_code"].notna() &
        df["year"].notna() &
        (df["trees_replaced"].fillna(0) > 0)
    )
    return df[ok].copy()


def _fetch_denominators(engine) -> pd.DataFrame:
    """
    Trae denominadores por (contract_code, year):
      - primero con planting_year
      - fallback con etp_year cuando falte planting_year
    Resultado con columnas: contract_code, year, trees_contract_den
    """
    sql = """
        WITH base AS (
            SELECT
                contract_code,
                planting_year,
                etp_year,
                trees_contract
            FROM masterdatabase.contract_tree_information
        ),
        by_py AS (
            SELECT contract_code,
                   planting_year AS year,
                   SUM(trees_contract)::float AS trees_contract
              FROM base
             WHERE planting_year IS NOT NULL
             GROUP BY contract_code, planting_year
        ),
        by_etp AS (
            SELECT contract_code,
                   etp_year AS year,
                   SUM(trees_contract)::float AS trees_contract
              FROM base
             WHERE etp_year IS NOT NULL
             GROUP BY contract_code, etp_year
        ),
        merged AS (
            -- preferimos planting_year, luego etp_year si falta
            SELECT COALESCE(py.contract_code, etp.contract_code) AS contract_code,
                   COALESCE(py.year, etp.year) AS year,
                   COALESCE(py.trees_contract, etp.trees_contract) AS trees_contract_den
              FROM by_py py
              FULL OUTER JOIN by_etp etp
                ON py.contract_code = etp.contract_code
               AND py.year = etp.year
        )
        SELECT contract_code, year::int AS year, trees_contract_den
          FROM merged
         WHERE year IS NOT NULL
    """
    return pd.read_sql(sql, engine)

from collections import OrderedDict

def _aggregate_ready(df_ready: pd.DataFrame) -> pd.DataFrame:
    # 1) Suma de árboles reemplazados
    agg = (
        df_ready.groupby(["contract_code", "year"], dropna=False)["trees_replaced"]
        .sum()
        .reset_index()
        .rename(columns={"trees_replaced": "replaced_count"})
    )

    # 2) Unión de especies sin normalizar, en orden de aparición, sin duplicados exactos
    def join_in_order(series: pd.Series) -> str | None:
        seen = OrderedDict()
        for x in series.tolist():
            if x is None or (isinstance(x, float) and pd.isna(x)):
                continue
            s = str(x)  # NO strip, NO lower: exactamente como viene
            if s not in seen:
                seen[s] = True
        if not seen:
            return None
        return " | ".join(seen.keys())

    sp = (
        df_ready.groupby(["contract_code", "year"], dropna=False)["species_strain"]
        .apply(join_in_order)
        .reset_index()
    )

    out = agg.merge(sp, on=["contract_code", "year"], how="left")
    return out  # -> columns: contract_code, year, replaced_count, species_strain


def _compute_pct(agg_df: pd.DataFrame, den_df: pd.DataFrame) -> pd.DataFrame:
    """
    pct_replaced = replaced_count / trees_contract_den (si > 0); si no hay denom, NULL.
    """
    out = agg_df.merge(den_df, on=["contract_code", "year"], how="left")
    out["pct_replaced"] = np.where(
        (out["trees_contract_den"] > 0),
        out["replaced_count"] / out["trees_contract_den"],
        np.nan
    )
    out = out.drop(columns=["trees_contract_den"])
    return out

def _upsert_cr_ts(engine, df: pd.DataFrame) -> Dict[str, int]:
    counts = {"upserts": 0}
    if df.empty:
        return counts

    records = df.to_dict(orient="records")

    def _nan_to_none(x):
        return None if pd.isna(x) else x

    with engine.begin() as conn:
        for rec in records:
            # Asegura None en campos opcionales
            rec["pct_replaced"]   = _nan_to_none(rec.get("pct_replaced"))
            rec["species_strain"] = _nan_to_none(rec.get("species_strain"))
            conn.execute(UPSERT_SQL, rec)
            counts["upserts"] += 1
    return counts

def process_contract_replacements_log(dry_run: bool = False) -> Dict[str, int]:
    """
    Lee ContractReplacementsLog(Ready) → agrega/une → calcula pct → UPSERT en cr_ts.
    Marca como DONE las filas Ready si no es dry_run (usando iter_ready_rows).
    """
    ensure_contract_replacements_sheet()

    engine = get_engine()

    # 1) Construye el DF de trabajo con tu helper (sin normalizar datos de usuario)
    df_ready = _read_ready_entries()
    stats = {"rows_ready": int(df_ready.shape[0]), "groups": 0, "upserts": 0, "marked_done": 0}

    if df_ready.empty:
        print("ℹ️ No hay filas Ready en ContractReplacementsLog.")
        return stats

    # 2) Agrega y calcula pct
    agg = _aggregate_ready(df_ready)
    stats["groups"] = int(agg.shape[0])

    den = _fetch_denominators(engine)
    out = _compute_pct(agg, den)

    if dry_run:
        print("👀 DRY-RUN → Preview de upserts a contract_replacements_ts:")
        for r in out.to_dict(orient="records"):
            print(f"  {r}")
        return stats

    # 3) UPSERT
    up = _upsert_cr_ts(engine, out)
    stats.update(up)

    # 4) Marcar como DONE usando el helper iter_ready_rows (consistencia con tus flujos)
    sheet = Sheet(CATALOG_FILE, SHEET_NAME)
    status_col = sheet.ensure_status_column("change_in_db")

    for r, _row in sheet.iter_ready_rows(status_col):
        sheet.mark_status(r, status_col, STATUS_DONE)
        stats["marked_done"] += 1

    sheet.save()

    print(f"✅ Ready={stats['rows_ready']} | groups={stats['groups']} | upserts={stats['upserts']} | marked_done={stats['marked_done']}")
    return stats


def main(dry_run: bool = False):
    ensure_contract_replacements_sheet()
    process_contract_replacements_log(dry_run=dry_run)

if __name__ == "__main__":
    main()
