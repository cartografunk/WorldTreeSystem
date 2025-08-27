from MasterDatabaseManagement.tools.minimal_parsers import _to_int, _to_date, _is_blank
from core.libs import text
from core.db import get_engine
from core.sheets import Sheet
from core.paths import DATABASE_EXPORTS_DIR
from core.libs import Path

CATALOG_FILE = Path(DATABASE_EXPORTS_DIR) / "changelog.xlsx"
SHEET_NAME = "NewContractInputLog"

sql_cti = text("""
    INSERT INTO masterdatabase.contract_tree_information
    (contract_code, planting_year, etp_year, harvest_year_10,
     trees_contract, planted, strain, status, planting_date, species, land_location_gps)
    VALUES
    (:contract_code, :planting_year, :etp_year, :harvest_year_10,
     :trees_contract, :planted, :strain, :status, :planting_date, :species, :land_location_gps)
    ON CONFLICT (contract_code) DO NOTHING
""")

def backfill_cti_from_sheet(dry_run=False):
    engine = get_engine()
    sheet = Sheet(CATALOG_FILE, SHEET_NAME)
    cc_idx     = sheet.ensure_column("Contract Code")
    status_idx = sheet.ensure_status_column("change_in_db")

    CTI_XFORM = {
        "plantingyear": _to_int,
        "treescontract": _to_int,
        "planted": _to_int,
        "plantingdate": _to_date,
    }

    inserted, skipped = 0, 0
    for r, row in sheet.iter_rows():
        status_val = sheet.get_cell(r, status_idx).value
        if not (status_val and str(status_val).strip().lower() == "done"):
            continue

        cc_cell = sheet.get_cell(r, cc_idx)
        contract_code = (cc_cell.value or "").strip()
        if not contract_code:
            skipped += 1
            continue

        # reconstruye cti_params
        cti_params = {}
        for k in ["plantingyear","treescontract","planted","strain","status","plantingdate","species","land_location_gps"]:
            val = sheet.read(row, k)
            if k in CTI_XFORM:
                val = CTI_XFORM[k](val)
            cti_params[k] = val

        py_num = _to_int(cti_params.get("plantingyear"))
        cti_db = {
            "contract_code": contract_code,
            "planting_year": py_num,
            "etp_year": py_num,
            "harvest_year_10": (py_num + 10) if py_num is not None else None,
            "trees_contract": cti_params.get("treescontract"),
            "planted": cti_params.get("planted"),
            "strain": cti_params.get("strain"),
            "status": cti_params.get("status") or "Pending",
            "planting_date": cti_params.get("plantingdate"),
            "species": cti_params.get("species"),
            "land_location_gps": cti_params.get("land_location_gps"),
        }

        if dry_run:
            print("DRY CTI:", cti_db)
            inserted += 1
        else:
            with engine.begin() as conn:
                conn.execute(sql_cti, cti_db)
            inserted += 1

    print(f"CTI backfill → insertados (o no-op por conflicto): {inserted} | saltados: {skipped} | dry_run={dry_run}")

# Lánzalo así:
# backfill_cti_from_sheet(dry_run=False)
