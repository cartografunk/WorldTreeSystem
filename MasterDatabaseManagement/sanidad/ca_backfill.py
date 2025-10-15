#MasterDatabaseManagement/sanidad/ca_backfill.py
from sqlalchemy import text
from core.db import get_engine

def upsert_ca_etp_from_cti(target_year: int | None = None) -> dict:
    """
    Backfill/UPSERT en masterdatabase.contract_allocation (ca) desde contract_tree_information (cti).
    - Inserta faltantes (cti.status='Active' y filtro por año) con:
        etp_type='ETP', contracted_cop=0, planted_cop=0,
        contracted_etp=cti.trees_contract, planted_etp=cti.planted,
        surviving_etp=0, surviving_cop=0.
    - Si la fila ya existe en ca: solo actualiza cuando ambos *_etp están en 0/NULL.
    - target_year=None  -> aplica a cti.etp_year > 2018
      target_year=int   -> aplica a ese año exacto.
    Retorna: {"remaining": N} con los que aún quedaron con *_etp en 0/NULL.
    """
    where_time = "cti.etp_year > 2018" if target_year is None else "cti.etp_year = :target_year"

    sql = f"""
    INSERT INTO masterdatabase.contract_allocation (
      contract_code, etp_type, loaded_at,
      contracted_cop, planted_cop,
      contracted_etp, planted_etp,
      surviving_etp, surviving_cop
    )
    SELECT
      cti.contract_code,
      'ETP', NOW(),
      0, 0,
      COALESCE(cti.trees_contract, 0),
      COALESCE(cti.planted, 0),
      0, 0
    FROM masterdatabase.contract_tree_information cti
    WHERE cti.status = 'Active'
      AND {where_time}
    ON CONFLICT (contract_code) DO UPDATE
    SET contracted_etp = COALESCE(masterdatabase.contract_allocation.contracted_etp, EXCLUDED.contracted_etp),
        planted_etp    = COALESCE(masterdatabase.contract_allocation.planted_etp,    EXCLUDED.planted_etp)
    WHERE COALESCE(masterdatabase.contract_allocation.contracted_etp,0) = 0
      AND COALESCE(masterdatabase.contract_allocation.planted_etp,0)    = 0;
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
