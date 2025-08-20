from core.libs import pd, Path
from sqlalchemy import text
from core.db import get_engine
from core.paths import DATABASE_EXPORTS_DIR
from core.region import get_prefix
from core.schema_helpers_db_management import extract_group_params
from core.sheets import Sheet
from core.backup import backup_tables, backup_excel

# === Config ===
CATALOG_FILE = Path(DATABASE_EXPORTS_DIR) / "changelog.xlsx"
SHEET_NAME = "NewContractInputLog_test"


def _fetch_max(engine, prefix: str) -> int:
    q = text("""
        SELECT COALESCE(MAX(CAST(SUBSTRING(contract_code FROM 3) AS INT)), 0) AS maxnum
        FROM masterdatabase.contract_farmer_information
        WHERE contract_code LIKE :pfx
    """)
    with engine.begin() as conn:
        val = conn.execute(q, {"pfx": f"{prefix}%"}).scalar()
    return int(val or 0)


# --- Parsers mínimos --------------------------------------------------------
def _to_int(v):
    n = pd.to_numeric(v, errors="coerce")
    return int(n) if pd.notna(n) else None

def _to_float(v):
    n = pd.to_numeric(v, errors="coerce")
    return float(n) if pd.notna(n) else None

def _to_date(v):
    if v is None or str(v).strip() == "":
        return None
    t = pd.to_datetime(v, errors="coerce", dayfirst=True)
    return t.date() if pd.notna(t) else None


# --- Validación mínima ------------------------------------------------------
REQUIRED_KEYS = ["region", "contractname", "plantingyear", "treescontract"]

def _is_ready(v) -> bool:
    return bool(v) and str(v).strip().lower() == "ready"

def _reason_for_skip(sheet: Sheet, row):
    def _blank(x):
        return x is None or (isinstance(x, str) and x.strip() == "")

    vals = {}
    missing = []
    for k in REQUIRED_KEYS:
        v = sheet.read(row, k)
        vals[k] = v
        if _blank(v):
            missing.append(k)
    if missing:
        return f"faltan campos {missing}", vals

    pfx = get_prefix(vals["region"])
    if not pfx:
        return f"prefijo inválido para region='{vals['region']}'", vals

    tc = pd.to_numeric(vals["treescontract"], errors="coerce")
    if pd.isna(tc):
        return f"treescontract no numérico: {vals['treescontract']}", vals

    py = pd.to_numeric(vals["plantingyear"], errors="coerce")
    if pd.isna(py):
        return f"plantingyear no numérico: {vals['plantingyear']}", vals

    return None, vals


def main(dry_run: bool = False):
    engine = get_engine()

    # Sheet centralizado
    sheet = Sheet(CATALOG_FILE, SHEET_NAME)
    cc_idx     = sheet.ensure_column("Contract Code")
    status_idx = sheet.ensure_column("change_in_db")  # ← columna de control única

    print(f"📄 Hoja {SHEET_NAME} tiene {sheet.ws.max_row - 1} filas de datos y {len(sheet.headers)} columnas")

    # Backups (solo en vivo)
    if not dry_run:
        backup_excel(CATALOG_FILE)
        backup_tables(
            engine,
            ["contract_farmer_information", "contract_tree_information"],
            schema="masterdatabase",
            label="pre_newcontracts"
        )

    # Transforms por grupo (aplicados por extract_group_params)
    CFI_XFORM = {"phone": lambda x: str(x).strip() if x else None}
    CTI_XFORM = {
        "plantingyear": _to_int,
        "harvest_year_10": _to_int,
        "treescontract": _to_int,
        "planted": _to_int,
        "plantingdate": _to_date,
        "latitude": _to_float,
        "longitude": _to_float,
    }

    # SQL (no destructivo)
    sql_cfi = text("""
        INSERT INTO masterdatabase.contract_farmer_information
        (contract_code, contract_name, representative, farmer_number, phone, email, address,
         shipping_address, region, status, notes)
        VALUES
        (:contract_code, :contract_name, :representative, :farmer_number, :phone, :email, :address,
         :shipping_address, :region, :status, :notes)
        ON CONFLICT (contract_code) DO NOTHING
    """)

    sql_cti = text("""
        INSERT INTO masterdatabase.contract_tree_information
        (contract_code, planting_year, etp_year, harvest_year_10, trees_contract, planted, strain,
         planting_date, species, latitude, longitude, land_location_gps)
        VALUES
        (:contract_code, :planting_year, :planting_year, :harvest_year_10, :trees_contract, :planted, :strain,
         :planting_date, :species, :latitude, :longitude, :land_location_gps)
        ON CONFLICT (contract_code) DO NOTHING
    """)

    counters = {}
    applied  = 0
    failed   = 0
    to_cfi_preview, to_cti_preview = [], []

    # Loop
    for r, row in sheet.iter_rows():
        print(f"➡️  Fila {r}: inspeccionando…")

        # ✅ Solo procesa si change_in_db == "Ready"
        status_val = sheet.get_cell(r, status_idx).value
        if not _is_ready(status_val):
            continue

        reason, vals = _reason_for_skip(sheet, row)
        if reason:
            print(f"⛔ Fila {r} descartada: {reason} | vals={vals}")
            continue

        region         = vals["region"]
        contract_name  = vals["contractname"]
        planting_year  = vals["plantingyear"]
        trees_contract = vals["treescontract"]

        # Contract Code existente / serializar por prefijo
        cc_cell = sheet.get_cell(r, cc_idx)
        contract_code = (cc_cell.value or "").strip() if cc_cell.value else ""
        if not contract_code:
            pfx = get_prefix(region)
            if pfx not in counters:
                counters[pfx] = _fetch_max(engine, pfx)
            counters[pfx] += 1
            contract_code = f"{pfx}{counters[pfx]:04d}"
            print(f"🆕 Fila {r}: asignado contract_code={contract_code}")
            if not dry_run:
                cc_cell.value = contract_code

        # Params por grupo usando schema/aliases
        cfi_params = extract_group_params(row, sheet.headers, sheet.hdr_df, "cfi", transforms=CFI_XFORM)
        cti_params = extract_group_params(row, sheet.headers, sheet.hdr_df, "cti", transforms=CTI_XFORM)

        # Ajustes CTI calculados
        py_num = _to_int(planting_year)
        cti_params["planting_year"] = py_num
        if cti_params.get("harvest_year_10") is None and py_num is not None:
            cti_params["harvest_year_10"] = py_num + 10

        print(f"🧩 Fila {r}: ready | region={region} | name={contract_name} | py={py_num} | trees={trees_contract}")

        if dry_run:
            to_cfi_preview.append({"contract_code": contract_code, **cfi_params})
            to_cti_preview.append({"contract_code": contract_code, **cti_params})
        else:
            try:
                with engine.begin() as conn:
                    conn.execute(sql_cfi, {"contract_code": contract_code, **cfi_params})
                    conn.execute(sql_cti, {"contract_code": contract_code, **cti_params})
                # ✅ marcar Done SOLO en change_in_db
                sheet.mark_done(r, status_idx, "Done")
                applied += 1
                print(f"✅ Fila {r} aplicada y marcada Done")
            except Exception as e:
                failed += 1
                print(f"💥 Fila {r} error: {e}")

    if not dry_run:
        sheet.save()

    if dry_run:
        if to_cfi_preview or to_cti_preview:
            print("=== Preview CFI (top 3) ===")
            print(pd.DataFrame(to_cfi_preview).head(3))
            print("=== Preview CTI (top 3) ===")
            print(pd.DataFrame(to_cti_preview).head(3))
        else:
            print("🪵 No hay preview: todas las filas fueron descartadas (revisa razones arriba).")

    print("✅ Contract Code serializado por prefijo y orden de fila.")
    print(f"✅ Filas marcadas como 'Done': {applied} | ❌ fallidos: {failed} | dry_run={dry_run}")


if __name__ == "__main__":
    main(dry_run=False)
