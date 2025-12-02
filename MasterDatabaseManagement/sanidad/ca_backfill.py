from sqlalchemy import text
from core.db import get_engine
from core.libs import pd, np
import logging

logger = logging.getLogger(__name__)



def upsert_ca_etp_from_cti(target_year: int | None = None) -> dict:
    """
    CORRECTED VERSION: Backfill contract_allocation with proper allocation logic.

    CHANGES FROM ORIGINAL:
    - Modern years (2024+) get etp_type='ETP', contracted_cop=0
    - Year 2015 gets etp_type='COP', contracted_etp=0
    - Year 2017 gets etp_type='COP', canada_2017_trees populated
    - NO percentage-based logic
    - Respects existing manual corrections (only updates if both *_etp are 0/NULL)

    SIGNATURE: UNCHANGED (preserves compatibility)
    """

    where_time = "cti.etp_year > 2018" if target_year is None else "cti.etp_year = :target_year"

    # ──────────────────────────────────────────────────────────────────────
    # CORRECTED SQL: Uses proper allocation logic based on etp_year
    # ──────────────────────────────────────────────────────────────────────

    sql = f"""
    INSERT INTO masterdatabase.contract_allocation (
      contract_code, etp_type, loaded_at,
      contracted_cop, planted_cop,
      contracted_etp, planted_etp,
      canada_2017_trees,
      surviving_etp, surviving_cop
    )
    SELECT
      cti.contract_code,
      -- CORRECTED: Proper etp_type based on year
      CASE 
        WHEN cti.etp_year = 2015 THEN 'COP'
        WHEN cti.etp_year = 2017 THEN 'COP'
        ELSE 'ETP'
      END,
      NOW(),
      -- CORRECTED: contracted_cop based on year
      CASE 
        WHEN cti.etp_year = 2015 THEN COALESCE(cti.trees_contract, 0)
        WHEN cti.etp_year = 2017 THEN COALESCE(cti.trees_contract, 0)
        ELSE 0
      END,
      -- CORRECTED: planted_cop based on year
      CASE 
        WHEN cti.etp_year = 2015 THEN COALESCE(cti.planted, 0)
        WHEN cti.etp_year = 2017 THEN COALESCE(cti.planted, 0)
        ELSE 0
      END,
      -- CORRECTED: contracted_etp based on year
      CASE 
        WHEN cti.etp_year IN (2015, 2017) THEN 0
        ELSE COALESCE(cti.trees_contract, 0)
      END,
      -- CORRECTED: planted_etp based on year
      CASE 
        WHEN cti.etp_year IN (2015, 2017) THEN 0
        ELSE COALESCE(cti.planted, 0)
      END,
      -- CORRECTED: canada_2017_trees for year 2017
      CASE 
        WHEN cti.etp_year = 2017 THEN COALESCE(cti.trees_contract, 0)
        ELSE 0
      END,
      0, 0
    FROM masterdatabase.contract_tree_information cti
    WHERE cti.status = 'Active'
      AND {where_time}
    ON CONFLICT (contract_code) DO UPDATE
    SET contracted_etp = CASE
            WHEN COALESCE(masterdatabase.contract_allocation.contracted_etp,0) = 0 
             AND COALESCE(masterdatabase.contract_allocation.planted_etp,0) = 0
            THEN EXCLUDED.contracted_etp
            ELSE masterdatabase.contract_allocation.contracted_etp
        END,
        planted_etp = CASE
            WHEN COALESCE(masterdatabase.contract_allocation.contracted_etp,0) = 0 
             AND COALESCE(masterdatabase.contract_allocation.planted_etp,0) = 0
            THEN EXCLUDED.planted_etp
            ELSE masterdatabase.contract_allocation.planted_etp
        END,
        contracted_cop = CASE
            WHEN COALESCE(masterdatabase.contract_allocation.contracted_etp,0) = 0 
             AND COALESCE(masterdatabase.contract_allocation.planted_etp,0) = 0
            THEN EXCLUDED.contracted_cop
            ELSE masterdatabase.contract_allocation.contracted_cop
        END,
        planted_cop = CASE
            WHEN COALESCE(masterdatabase.contract_allocation.contracted_etp,0) = 0 
             AND COALESCE(masterdatabase.contract_allocation.planted_etp,0) = 0
            THEN EXCLUDED.planted_cop
            ELSE masterdatabase.contract_allocation.planted_cop
        END,
        etp_type = CASE
            WHEN COALESCE(masterdatabase.contract_allocation.contracted_etp,0) = 0 
             AND COALESCE(masterdatabase.contract_allocation.planted_etp,0) = 0
            THEN EXCLUDED.etp_type
            ELSE masterdatabase.contract_allocation.etp_type
        END,
        canada_2017_trees = CASE
            WHEN COALESCE(masterdatabase.contract_allocation.contracted_etp,0) = 0 
             AND COALESCE(masterdatabase.contract_allocation.planted_etp,0) = 0
            THEN EXCLUDED.canada_2017_trees
            ELSE masterdatabase.contract_allocation.canada_2017_trees
        END
    WHERE COALESCE(masterdatabase.contract_allocation.contracted_etp,0) = 0
      AND COALESCE(masterdatabase.contract_allocation.planted_etp,0) = 0;
    """

    qa = f"""
    SELECT COUNT(*) AS remaining
    FROM masterdatabase.contract_allocation ca
    JOIN masterdatabase.contract_tree_information cti
      ON ca.contract_code = cti.contract_code
    WHERE cti.status = 'Active'
      AND {where_time}
      AND (COALESCE(ca.contracted_etp,0) = 0 OR COALESCE(ca.planted_etp,0) = 0);
    """

    engine = get_engine()
    with engine.begin() as cx:
        params = {} if target_year is None else {"target_year": target_year}
        cx.execute(text(sql), params)
        remaining = cx.execute(text(qa), params).scalar_one()
        return {"remaining": int(remaining)}

def repair_modern_year_allocations() -> dict:
    """
    NEW FUNCTION: Zero out COP for modern years (2024+).

    This is a CRITICAL repair function that should be run ONCE
    to fix any corrupted modern year data.

    Returns:
        dict: {"repaired": count}
    """

    engine = get_engine()

    repair_sql = text("""
        UPDATE masterdatabase.contract_allocation ca
        SET 
            contracted_cop = 0,
            planted_cop = 0,
            etp_type = 'ETP',
            loaded_at = NOW()
        FROM masterdatabase.contract_tree_information cti
        WHERE ca.contract_code = cti.contract_code
          AND cti.etp_year >= 2024
          AND (ca.contracted_cop != 0 OR ca.etp_type != 'ETP')
    """)

    with engine.begin() as conn:
        result = conn.execute(repair_sql)
        repaired = result.rowcount

    print(f"🚨 CRITICAL REPAIR: Zeroed COP for {repaired} modern year contracts")
    return {"repaired": repaired}
