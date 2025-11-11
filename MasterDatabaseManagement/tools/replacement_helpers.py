# MasterDatabaseManagement/tools/replacements_helpers.py
from __future__ import annotations
from typing import Dict
from collections import OrderedDict
from core.libs import pd, np, text
from core.db import get_engine

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

def fetch_denominators() -> pd.DataFrame:
    """Trae denominadores por (contract_code, year) prefiriendo planting_year y luego etp_year."""
    eng = get_engine()
    sql = """
        WITH base AS (
            SELECT contract_code, planting_year, etp_year, trees_contract
            FROM masterdatabase.contract_tree_information
        ),
        by_py AS (
            SELECT contract_code, planting_year AS year, SUM(trees_contract)::float AS trees_contract
              FROM base
             WHERE planting_year IS NOT NULL
             GROUP BY contract_code, planting_year
        ),
        by_etp AS (
            SELECT contract_code, etp_year AS year, SUM(trees_contract)::float AS trees_contract
              FROM base
             WHERE etp_year IS NOT NULL
             GROUP BY contract_code, etp_year
        ),
        merged AS (
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
    return pd.read_sql(sql, eng)

def aggregate_ready(df_ready: pd.DataFrame) -> pd.DataFrame:
    agg = (
        df_ready.groupby(["contract_code", "year"], dropna=False)["trees_replaced"]
        .sum()
        .reset_index()
        .rename(columns={"trees_replaced": "replaced_count"})
    )
    # especies “passthrough” uniendo sin duplicados exactos y conservando orden
    def join_in_order(series: pd.Series) -> str | None:
        seen = OrderedDict()
        for x in series.tolist():
            if x is None or (isinstance(x, float) and pd.isna(x)):
                continue
            s = str(x)  # no strip/lower
            if s not in seen:
                seen[s] = True
        return " | ".join(seen.keys()) if seen else None

    sp = (
        df_ready.groupby(["contract_code", "year"], dropna=False)["species_strain"]
        .apply(join_in_order)
        .reset_index()
    )
    return agg.merge(sp, on=["contract_code", "year"], how="left")

def compute_pct(agg_df: pd.DataFrame, den_df: pd.DataFrame) -> pd.DataFrame:
    out = agg_df.merge(den_df, on=["contract_code", "year"], how="left")
    out["pct_replaced"] = np.where(
        (out["trees_contract_den"] > 0),
        out["replaced_count"] / out["trees_contract_den"],
        np.nan
    )
    return out.drop(columns=["trees_contract_den"])

def upsert_cr_ts(df: pd.DataFrame) -> Dict[str, int]:
    eng = get_engine()
    cnt = 0
    if df.empty:
        return {"upserts": 0}
    recs = df.to_dict(orient="records")
    def _none(x):
        from math import isnan
        try:
            return None if pd.isna(x) else x
        except Exception:
            return x
    with eng.begin() as conn:
        for r in recs:
            r["pct_replaced"]   = _none(r.get("pct_replaced"))
            r["species_strain"] = _none(r.get("species_strain"))
            conn.execute(UPSERT_SQL, r)
            cnt += 1
    return {"upserts": cnt}
