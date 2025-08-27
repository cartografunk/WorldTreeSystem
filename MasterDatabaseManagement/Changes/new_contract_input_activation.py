# MasterDatabaseManagement/Changes/new_contract_input_activation.py
from MasterDatabaseManagement.tools.minimal_parsers import _to_int, _to_date, _is_blank
from core.libs import pd, Path, text
from core.db import get_engine
from core.paths import DATABASE_EXPORTS_DIR
from core.region import get_prefix
from core.schema_helpers_db_management import extract_group_params
from core.sheets import Sheet
from core.backup import backup_tables, backup_excel
from typing import Optional  # si no lo tienes aún

# === Config ===
CATALOG_FILE = Path(DATABASE_EXPORTS_DIR) / "changelog.xlsx"
SHEET_NAME = "NewContractInputLog"


def _fetch_max(engine, prefix: str) -> int:
    """
    Obtiene el mayor correlativo existente para el prefijo (US/MX/CR/GT)
    leyendo exclusivamente de FPI.contract_codes (array de TEXT).
    Asume códigos tipo 'US0001', 'CR0112', etc. (prefijo de 2 letras).
    """
    pfx = f"{prefix.upper()}%"
    q = text("""
        SELECT COALESCE(MAX(CAST(SUBSTRING(cc FROM 3) AS INT)), 0) AS maxnum
        FROM (
          SELECT UNNEST(COALESCE(contract_codes, ARRAY[]::text[])) AS cc
          FROM masterdatabase.farmer_personal_information
        ) t
        WHERE cc LIKE :pfx
    """)
    with engine.begin() as conn:
        val = conn.execute(q, {"pfx": pfx}).scalar()
    return int(val or 0)


# --- Snapshot CFI por farmer_number (para clonado total) --------------------
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
                (
                    SELECT ch.contract_name
                    FROM masterdatabase.contract_header ch
                    WHERE ch.farmer_number = fpi.farmer_number
                      AND ch.contract_name IS NOT NULL
                    ORDER BY ch.contract_code DESC
                    LIMIT 1
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


# --- Validación mínima: numéricos + region para prefijo ---------------------
REQUIRED_KEYS = ["region", "plantingyear", "treescontract"]

def _reason_for_skip(sheet: Sheet, row):
    def _blank(x):
        return x is None or (isinstance(x, str) and x.strip() == "")

    vals = {}
    for k in REQUIRED_KEYS:
        vals[k] = sheet.read(row, k)

    # region obligatorio (solo para prefijo)
    if _blank(vals["region"]):
        return "region requerida para prefijo de contract_code", vals

    tc = pd.to_numeric(vals.get("treescontract"), errors="coerce")
    py = pd.to_numeric(vals.get("plantingyear"), errors="coerce")
    if pd.isna(tc) or pd.isna(py):
        return "treescontract/plantingyear deben ser numéricos", vals

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
            ["farmer_personal_information", "contract_tree_information"],
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
    sql_cti = text("""
        INSERT INTO masterdatabase.contract_tree_information
        (contract_code, planting_year, etp_year, harvest_year,
         trees_contract, planted, strain, status, planting_date, species, land_location)
        VALUES
        (:contract_code, :planting_year, :etp_year, :harvest_year_10,
         :trees_contract, :planted, :strain, :status, :planting_date, :species, :land_location_gps)
        ON CONFLICT (contract_code) DO NOTHING
    """)

    # --- Farmer Personal Information (FPI) --------------------------------------
    sql_fpi_insert = text("""
        INSERT INTO masterdatabase.farmer_personal_information
            (farmer_number, representative, phone, email, address, shipping_address, contract_codes)
        VALUES
            (:farmer_number, :representative, :phone, :email, :address, :shipping_address, ARRAY[:contract_code]::text[])
        ON CONFLICT (farmer_number) DO NOTHING
    """)

    # Append del contract_code al array (evita duplicar si ya está)
    sql_fpi_append_code = text("""
        UPDATE masterdatabase.farmer_personal_information
           SET contract_codes = CASE
               WHEN contract_codes IS NULL
                 THEN ARRAY[:contract_code]::text[]
               WHEN array_position(contract_codes, :contract_code) IS NULL
                 THEN contract_codes || ARRAY[:contract_code]::text[]
               ELSE contract_codes
           END
         WHERE farmer_number = :farmer_number
    """)

    counts = {"applied": 0, "failed": 0, "not_ready": 0, "skipped": 0}
    to_cfi_preview, to_cti_preview = [], []
    counters = {}

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

        # Datos básicos del sheet (pueden venir incompletos si es Escenario 2)
        planting_year = vals["plantingyear"]
        trees_contract = vals["treescontract"]
        contract_name = sheet.read(row, "contractname")  # puede estar vacío
        region_sheet = sheet.read(row, "region")
        farmer_number_in_sheet = str(sheet.read(row, "farmernumber") or "").strip()

        # === Determina si el farmer ya existe y arma escenario ======================
        with engine.begin() as conn:
            existing_cfi = _fetch_personal_snapshot_by_farmer(conn,
                                                         farmer_number_in_sheet) if farmer_number_in_sheet else None

        # Region: SOLO viene del sheet (en BD no existe)
        region = region_sheet
        if _is_blank(region):
            print(f"⛔ Fila {r} descartada: region requerida (solo del sheet).")
            counts["skipped"] += 1
            continue

        # Siempre generamos contract_code nuevo (este flujo es solo ALTAS)
        cc_cell = sheet.get_cell(r, cc_idx)
        pfx = get_prefix(region)
        if not pfx:
            print(f"⛔ Fila {r} descartada: prefijo inválido para region='{region}'.")
            counts["skipped"] += 1
            continue

        if pfx not in counters:
            counters[pfx] = _fetch_max(engine, pfx)
        counters[pfx] += 1
        contract_code = f"{pfx}{counters[pfx]:04d}"
        print(f"🆕 Fila {r}: asignado contract_code={contract_code}")
        if not dry_run:
            cc_cell.value = contract_code

        # === Escenarios: CFI desde sheet (nuevo) VS clonado (existente) ============
        if existing_cfi is None:
            # Escenario 1: Farmer nuevo → CFI desde sheet
            cfi_params = extract_group_params(row, sheet.headers, sheet.hdr_df, "cfi", transforms=CFI_XFORM)
            # Si contract_name viene vacío en el sheet pero te lo pasaron por variable, úsalo
            if _is_blank(cfi_params.get("contract_name")) and not _is_blank(contract_name):
                cfi_params["contract_name"] = contract_name
        else:
            # Escenario 2: Farmer existente → clonar CFI 1:1 desde BD
            cfi_params = {
                "representative": existing_cfi.get("representative"),
                "farmer_number": existing_cfi.get("farmer_number"),
                "phone": existing_cfi.get("phone"),
                "email": existing_cfi.get("email"),
                "address": existing_cfi.get("address"),
                "shipping_address": existing_cfi.get("shipping_address"),
                "contract_name": existing_cfi.get("contract_name") if _is_blank(contract_name) else contract_name,
            }

        # CTI (sin lat/long; harvest derivado; etp=planting)
        cti_params = extract_group_params(row, sheet.headers, sheet.hdr_df, "cti", transforms=CTI_XFORM)
        py_num = _to_int(planting_year)
        cti_params["planting_year"] = py_num
        cti_params["etp_year"] = py_num
        cti_params["harvest_year_10"] = (py_num + 10) if py_num is not None else None

        # status ahora vive en CTI; si no viene, defaultea
        if _is_blank(cti_params.get("status")):
            cti_params["status"] = "Pending"

        # Para evitar pasar claves extra al INSERT de CFI (por si cambian columnas)
        ALLOWED_CFI_KEYS = {
            "contract_name",
            "representative",
            "farmer_number",
            "phone",
            "email",
            "address",
            "shipping_address",
        }

        with engine.begin() as conn:
            if dry_run:
                to_fpi_preview.append({
                    "farmer_number": farmer_number_in_sheet or "(sin FN)",
                    "will_append_code": contract_code,
                    "representative": cfi_params.get("representative"),
                    "phone": cfi_params.get("phone"),
                    "email": cfi_params.get("email"),
                    "address": cfi_params.get("address"),
                    "shipping_address": cfi_params.get("shipping_address"),
                })
                to_cti_preview.append({"contract_code": contract_code, **cti_params})
            else:
                try:
                    # ====== FPI: alta/append de contract_code en la lista (si hay farmer_number) ======
                    if farmer_number_in_sheet:
                        # 1) Inserta FPI si es farmer nuevo (inicializa contract_codes = [contract_code])
                        conn.execute(sql_fpi_insert, {
                            "farmer_number": farmer_number_in_sheet,
                            "representative": cfi_params.get("representative"),
                            "phone": cfi_params.get("phone"),
                            "email": cfi_params.get("email"),
                            "address": cfi_params.get("address"),
                            "shipping_address": cfi_params.get("shipping_address"),
                            "contract_code": contract_code,
                        })
                        # 2) Asegura que el contract_code quede en la lista (sin duplicar)
                        conn.execute(sql_fpi_append_code, {
                            "farmer_number": farmer_number_in_sheet,
                            "contract_code": contract_code,
                        })

                    # ====== CTI: alta no destructiva por contract_code ======
                    cti_db = {
                        "contract_code": contract_code,
                        "planting_year": cti_params.get("planting_year"),
                        "etp_year": cti_params.get("etp_year"),
                        "trees_contract": cti_params.get("trees_contract"),
                        "planted": cti_params.get("planted"),
                        "strain": cti_params.get("strain"),
                        "status": cti_params.get("status"),
                        "planting_date": cti_params.get("planting_date"),
                        "species": cti_params.get("species"),
                        "land_location_gps": cti_params.get("land_location_gps"),
                        "harvest_year_10": cti_params.get("harvest_year_10")
                    }
                    conn.execute(sql_cti, cti_db)

                    sheet.mark_status(r, status_idx, "Done")
                    counts["applied"] += 1
                    print(
                        f"✅ Fila {r} aplicada y marcada Done (escenario={'nuevo' if existing_cfi is None else 'clonado'})")
                except Exception as e:
                    counts["failed"] += 1
                    print(f"💥 Fila {r} error: {e}")

    if not dry_run:
        sheet.save()

    if dry_run:
        if to_fpi_preview:
            print("=== Preview FPI ===")
            print(pd.DataFrame(to_fpi_preview))
        if to_cti_preview:
            print("=== Preview CTI (top 3) ===")
            print(pd.DataFrame(to_cti_preview))
        else:
            print("🪵 No hay preview: todas las filas fueron descartadas (revisa razones arriba).")

    print("✅ Contract Code serializado por prefijo y orden de fila.")
    print(f"✅ Filas marcadas como 'Done': {counts['applied']} | ❌ fallidos: {counts['failed']} | no_ready: {counts['not_ready']} | skipped: {counts['skipped']} | dry_run={dry_run}")
