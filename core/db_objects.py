# core/db_objects.py
from sqlalchemy import text
from core.db import get_engine

def _get_columns(engine, schema: str, table: str) -> dict:
    sql = """
    SELECT lower(column_name) AS column_name, data_type, udt_name
    FROM information_schema.columns
    WHERE table_schema = :schema AND table_name = :table
    """
    with engine.begin() as conn:
        rows = conn.execute(text(sql), {"schema": schema, "table": table}).mappings().all()
    return {r["column_name"]: (r["data_type"], r["udt_name"]) for r in rows}

def ensure_fpi_expanded_view(engine=None, grant_to: str | None = None):
    """
    Crea/actualiza la vista masterdatabase.fpi_contracts_expanded expandiendo
    farmer_personal_information.contract_codes (ARRAY) o usando contract_code (texto).
    Detecta automáticamente la columna de región (region/country/region_name/...).
    Si no hay ninguna, pone NULL::text AS region.
    """
    engine = engine or get_engine()

    cols = _get_columns(engine, "masterdatabase", "farmer_personal_information")
    names = set(cols.keys())

    # 1) contract_code: array vs escalar
    if "contract_codes" in names:
        contract_expr = "UNNEST(fpi.contract_codes)"
    elif "contract_code" in names:
        contract_expr = "fpi.contract_code"
    else:
        raise RuntimeError(
            "❌ En masterdatabase.farmer_personal_information no encontré ni 'contract_codes' (ARRAY) ni 'contract_code'."
        )

    # 2) region: elige la primera que exista
    region_candidates = [
        "region", "country", "region_name", "country_name",
        "country_region", "countryregion", "location"
    ]
    region_expr = None
    for cand in region_candidates:
        if cand in names:
            region_expr = f"fpi.{cand}"
            break
    if not region_expr:
        region_expr = "NULL::text"

    sql = f"""
    CREATE SCHEMA IF NOT EXISTS masterdatabase;

    CREATE OR REPLACE VIEW masterdatabase.fpi_contracts_expanded AS
    SELECT
      {contract_expr} AS contract_code,
      {region_expr}   AS region
    FROM masterdatabase.farmer_personal_information fpi;
    """

    with engine.begin() as conn:
        conn.execute(text(sql))
        if grant_to:
            conn.execute(text(f"GRANT SELECT ON masterdatabase.fpi_contracts_expanded TO {grant_to};"))
