# SOLO reemplaza el archivo por este:

from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
from sqlalchemy import text
import pandas as pd
from core.sheets import get_table_for_field

PERSONAL_INFO_REASON_ID = 5  # razón 5 = personal information change
# Campos personales permitidos a propagar en CFI
PERSONAL_INFO_ALLOWED = {
    "representative",
    "phone",
    "email",
    "address",
    "shipping_address",
    "contract_name"
}

def _to_int_or_none(v) -> Optional[int]:
    try:
        return int(str(v).strip())
    except Exception:
        return None

@dataclass
class ChangeResult:
    ok: bool
    mode: str   # 'single' | 'propagated' | 'skipped_no_table' | 'skipped_missing_farmer' | 'skipped_not_personal'
    info: str = ""

def apply_changelog_change(
    conn,
    *,
    contract_code: str,
    target_field: str,
    change_val,
    reason_val,
    fields_catalog: pd.DataFrame,
) -> ChangeResult:
    """
    - Resuelve tabla con fields_catalog.
    - Si reason_id == 5 y target_field es PERSONAL_INFO_ALLOWED en CFI → propaga a todos los contratos del mismo farmer_number.
    - Si no, update solo por contract_code.
    """
    table = get_table_for_field(fields_catalog, target_field)
    if not table:
        return ChangeResult(False, "skipped_no_table", f"Campo {target_field} sin tabla en catalog")

    reason_id = _to_int_or_none(reason_val)

    # ¿Propagar? solo si es CFI + razón 5 + campo personal permitido
    tf_norm = str(target_field).strip().lower().replace(" ", "_")
    wants_propagate = (table == "contract_farmer_information"
                       and reason_id == PERSONAL_INFO_REASON_ID
                       and tf_norm in PERSONAL_INFO_ALLOWED)

    if wants_propagate:
        farmer_number = conn.execute(
            text("""
                SELECT farmer_number
                FROM masterdatabase.contract_farmer_information
                WHERE contract_code = :cc
                LIMIT 1
            """),
            {"cc": str(contract_code)},
        ).scalar()

        if not farmer_number:
            # cae a single update si no hay farmer_number
            stmt = text(f'''
                UPDATE masterdatabase."{table}"
                   SET "{target_field}" = :val
                 WHERE contract_code = :cc
            ''')
            conn.execute(stmt, {"val": change_val, "cc": str(contract_code)})
            return ChangeResult(True, "skipped_missing_farmer", "Sin farmer_number; solo ese contrato")

        stmt = text(f'''
            UPDATE masterdatabase."{table}"
               SET "{target_field}" = :val
             WHERE farmer_number = :fn
        ''')
        conn.execute(stmt, {"val": change_val, "fn": str(farmer_number)})
        return ChangeResult(True, "propagated", f"Propagado por farmer_number={farmer_number}")

    # Default: UPDATE por contract_code
    stmt = text(f'''
        UPDATE masterdatabase."{table}"
           SET "{target_field}" = :val
         WHERE contract_code = :cc
    ''')
    conn.execute(stmt, {"val": change_val, "cc": str(contract_code)})
    return ChangeResult(True, "single", "Actualización por contract_code")
