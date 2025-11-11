# MasterDatabaseManagement/tools/fpi_cti_helpers.py
from typing import Dict, Any, Optional
from sqlalchemy import text
from core.db import get_engine


def fetch_next_farmer_number(region: str) -> str:  # ← cambiar return type a str
    """
    Asigna farmer_number secuencial por región:
    US=1xxxx, CR=2xxxx, GT=3xxxx, MX=4xxxx
    Retorna STRING para consistencia con la BD.
    """
    engine = get_engine()

    region_map = {"US": 1, "CR": 2, "GT": 3, "MX": 4}
    if region not in region_map:
        raise ValueError(f"Región inválida: {region}")

    prefix = region_map[region] * 10000
    upper = prefix + 9999

    sql = text("""
        SELECT COALESCE(MAX(farmer_number::integer), :prefix) AS maxnum
        FROM masterdatabase.farmer_personal_information
        WHERE farmer_number::integer BETWEEN :prefix AND :upper
    """)

    with engine.begin() as conn:
        val = conn.execute(sql, {"prefix": prefix, "upper": upper}).scalar()

    next_num = int(val or prefix) + 1
    return str(next_num)  # ← CONVERTIR A STRING antes de retornar

def fetch_max_contract_seq(prefix2: str) -> int:
    pfx = f"{prefix2.upper()}%"
    sql = """
        SELECT COALESCE(MAX(CAST(SUBSTRING(cc FROM 3) AS INT)), 0) AS maxnum
        FROM (
          SELECT UNNEST(COALESCE(contract_codes, ARRAY[]::text[])) AS cc
          FROM masterdatabase.farmer_personal_information
        ) t
        WHERE cc LIKE :pfx
    """
    eng = get_engine()
    with eng.begin() as conn:
        val = conn.execute(text(sql), {"pfx": pfx}).scalar()
    return int(val or 0)

def fpi_insert_or_append(fpi: Dict[str, Any], contract_code: str) -> None:
    eng = get_engine()
    sql_insert = text("""
        INSERT INTO masterdatabase.farmer_personal_information
            (farmer_number, representative, phone, email, address, shipping_address, contract_name, contract_codes)
        VALUES
            (:farmer_number, :representative, :phone, :email, :address, :shipping_address, :contract_name, ARRAY[:contract_code]::text[])
        ON CONFLICT (farmer_number) DO NOTHING
    """)
    sql_append = text("""
        UPDATE masterdatabase.farmer_personal_information
        SET contract_codes = array_append(contract_codes, :contract_code)
        WHERE farmer_number = :farmer_number
          AND NOT (:contract_code = ANY(contract_codes))
    """)
    with eng.begin() as conn:
        conn.execute(sql_insert, {**fpi, "contract_code": contract_code})
        conn.execute(sql_append, {"farmer_number": fpi["farmer_number"], "contract_code": contract_code})

def cti_insert(cti: Dict[str, Any]) -> None:
    eng = get_engine()
    sql = text("""
        INSERT INTO masterdatabase.contract_tree_information
        (contract_code, planting_year, etp_year, harvest_year,
         trees_contract, planted, strain, status, planting_date, species, land_location)
        VALUES
        (:contract_code, :planting_year, :etp_year, :harvest_year_10,
         :trees_contract, :planted, :strain, :status, :planting_date, :species, :land_location_gps)
        ON CONFLICT (contract_code) DO NOTHING
    """)
    with eng.begin() as conn:
        conn.execute(sql, cti)


def _fetch_personal_snapshot_by_farmer(conn, farmer_number: str) -> Optional[dict]:
    """
    Trae datos PERSONALES desde farmer_personal_information.
    Además, intenta traer el último contract_name desde contract_header.
    """
    if not farmer_number:
        return None
    try:
        row = conn.execute(text("""
            SELECT
                fpi.farmer_number,
                fpi.representative,
                fpi.phone,
                fpi.email,
                fpi.address,
                fpi.shipping_address,
                COALESCE(
                    fpi.contract_name,
                    (
                        SELECT ch.contract_name
                        FROM masterdatabase.contract_header ch
                        WHERE ch.farmer_number = fpi.farmer_number
                          AND ch.contract_name IS NOT NULL
                        ORDER BY ch.contract_code DESC
                        LIMIT 1
                    )
                ) AS contract_name
            FROM masterdatabase.farmer_personal_information fpi
            WHERE fpi.farmer_number = :fn
            LIMIT 1
        """), {"fn": str(farmer_number)}).mappings().first()

        return dict(row) if row else None
    except Exception:
        # Fallback legacy si CH/FPI aún no existen
        row = conn.execute(text("""
            SELECT representative, farmer_number, phone, email, address,
                   shipping_address, contract_name
            FROM masterdatabase.contract_farmer_information
            WHERE farmer_number = :fn
            ORDER BY contract_code DESC
            LIMIT 1
        """), {"fn": str(farmer_number)}).mappings().first()
        return dict(row) if row else None
