# MasterDatabaseManagement/Changes/new_contract_input_activation.py

from core.libs import pd, Path, text
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

def _is_blank(v) -> bool:
    if v is None:
        return True
    s = str(v).replace("\u00A0", " ").strip()
    return s == ""


# --- Prefill CFI desde BD si farmer ya existe -------------------------------
# columnas personales en CFI que SÍ queremos “heredar” si faltan en el sheet
CFI_PERSONAL_COLS = [
    "representative",
    "farmer_number",   # lo mantiene tal cual; si falta en sheet y existe, lo usamos
    "phone",
    "email",
    "address",
    "shipping_address",
    "contract_name"
    # OJO: NO copiamos contract_name (ese es propio del nuevo contrato)
]

def _prefill_cfi_from_existing(conn, cfi_params: dict, cache: dict) -> dict:
    """
    Si farmer_number existe en BD, rellena en cfi_params los campos personales que vengan vacíos.
    Usa cache por farmer_number para no consultar repetido.
    """
    fn = cfi_params.get("farmer_number")
    if _is_blank(fn):
        return cfi_params

    key = str(fn)
    base = cache.get(key)

    if base is None:
        # Trae snapshot del farmer (una fila cualquiera) con columnas personales
        cols_sql = ", ".join(f'"{c}"' for c in CFI_PERSONAL_COLS if c != "farmer_number")  # farmer_number igual lo tenemos
        sql = text(f"""
            SELECT farmer_number, {cols_sql}
            FROM masterdatabase.contract_farmer_information
            WHERE farmer_number = :fn
            LIMIT 1
        """)
        row = conn.execute(sql, {"fn": str(fn)}).mappings().first()
        base = dict(row) if row else {}
        cache[key] = base

    if base:
        for col in CFI_PERSONAL_COLS:
            if _is_blank(cfi_params.get(col)) and (base.get(col) is not None):
                cfi_params[col] = base[col]

    return cfi_params


# --- Validación mínima ------------------------------------------------------
REQUIRED_KEYS = ["region", "contractname", "plantingyear", "treescontract"]

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
    status_idx = sheet.ensure_status_column("change_in_db")  # usamos change_in_db: Ready -> Done

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

    # Transforms
    CFI_XFORM = {"phone": lambda x: str(x).strip() if x else None}
    CTI_XFORM = {
        "plantingyear": _to_int,
        "treescontract": _to_int,
        "planted": _to_int,
        "plantingdate": _to_date
    }

    # SQL inserts (no destructivo)
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

    counts = {"applied": 0, "failed": 0, "not_ready": 0, "skipped": 0}
    to_cfi_preview, to_cti_preview = [], []
    counters = {}
    cfi_cache = {}  # farmer_number -> snapshot de CFI en BD

    def _is_ready(v) -> bool:
        return bool(v) and str(v).replace("\u00A0", " ").strip().lower() == "ready"

    for r, row in sheet.iter_rows():
        print(f"➡️  Fila {r}: inspeccionando…")

        status_val = sheet.get_cell(r, status_idx).value
        if not _is_ready(status_val):
            counts["not_ready"] += 1
            continue

        reason, vals = _reason_for_skip(sheet, row)
        if reason:
            print(f"⛔ Fila {r} descartada: {reason} | vals={vals}")
            counts["skipped"] += 1
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

        # Params por grupo (con transforms)
        cfi_params = extract_group_params(row, sheet.headers, sheet.hdr_df, "cfi", transforms=CFI_XFORM)
        cti_params = extract_group_params(row, sheet.headers, sheet.hdr_df, "cti", transforms=CTI_XFORM)

        # Ajustes CTI calculados
        py_num = _to_int(planting_year)
        cti_params["planting_year"] = py_num
        if cti_params.get("harvest_year_10") is None and py_num is not None:
            cti_params["harvest_year_10"] = py_num + 10

        # 🔎 PREFILL CFI desde BD si farmer_number ya existe (sin pisar lo que venga en el sheet)
        with engine.begin() as conn:
            cfi_params = _prefill_cfi_from_existing(conn, cfi_params, cfi_cache)

            if dry_run:
                to_cfi_preview.append({"contract_code": contract_code, **cfi_params})
                to_cti_preview.append({"contract_code": contract_code, **cti_params})
            else:
                try:
                    conn.execute(sql_cfi, {"contract_code": contract_code, **cfi_params})
                    conn.execute(sql_cti, {"contract_code": contract_code, **cti_params})
                    sheet.mark_status(r, status_idx, "Done")
                    counts["applied"] += 1
                    print(f"✅ Fila {r} aplicada y marcada Done")
                except Exception as e:
                    counts["failed"] += 1
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
    print(f"✅ Filas marcadas como 'Done': {counts['applied']} | ❌ fallidos: {counts['failed']} | no_ready: {counts['not_ready']} | skipped: {counts['skipped']} | dry_run={dry_run}")
