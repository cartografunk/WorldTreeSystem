# MasterDatabaseManagement/Changes/new_contract_input_activation.py
from MasterDatabaseManagement.tools.minimal_parsers import _to_int, _to_date, _is_blank
from core.libs import pd, Path, shutil
from core.db import get_engine
from core.paths import DATABASE_EXPORTS_DIR, ensure_all_paths_exist
from core.region import get_prefix
from core.schema_helpers_db_management import extract_group_params
from core.sheets import Sheet, export_tables_to_excel
from core.backup_control import backup_once, rotate_excel_backup_single_latest
from core.sync import sync_contract_allocation_full  # 👈 Agregar import

# ✅ Helpers centralizados
from ..tools.fpi_cti_helpers import (
    fetch_next_farmer_number, fetch_max_contract_seq,
    fpi_insert_or_append, cti_insert, _fetch_personal_snapshot_by_farmer
)
from MasterDatabaseManagement.tools.database_management_helpers import log_new_contract_to_changelog


# === Config ===
CATALOG_FILE = Path(DATABASE_EXPORTS_DIR) / "changelog.xlsx"
SHEET_NAME   = "NewContractInputLog"
EXCEL_FILE   = Path(DATABASE_EXPORTS_DIR) / "masterdatabase_export.xlsx"

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
    ensure_all_paths_exist()

    to_fpi_preview, to_cti_preview, to_ca_preview = [], [], []
    years_touched = set()

    tables_to_backup = [
        ("masterdatabase", "contract_farmer_information"),
        ("masterdatabase", "contract_tree_information"),
    ]
    df_backup_log = backup_once(tables_to_backup, tag="new_contracts_input", engine=engine)

    sheet = Sheet(CATALOG_FILE, SHEET_NAME)
    cc_idx = sheet.ensure_column("Contract Code")
    status_idx = sheet.ensure_status_column("change_in_db")

    print(f"📄 Hoja {SHEET_NAME} tiene {sheet.ws.max_row - 1} filas de datos y {len(sheet.headers)} columnas")

    CFI_XFORM = {"phone": lambda x: str(x).strip() if x else None}
    CTI_XFORM = {
        "plantingyear": _to_int,
        "treescontract": _to_int,
        "planted": _to_int,
        "plantingdate": _to_date
    }

    counts = {"applied": 0, "failed": 0, "not_ready": 0, "skipped": 0}
    counters: dict[str, int] = {}  # ← mantiene correlativo por prefijo

    contracts_created = []  # 👈 Trackear contratos creados

    # ========== LOOP PRINCIPAL ==========
    for r, row in sheet.iter_rows():
        status_val = sheet.get_cell(r, status_idx).value
        status_str = str(status_val).strip().lower() if status_val else ""

        # 🔒 Saltar filas ya procesadas
        if status_str == "done":
            continue

        # Solo procesar filas "Ready"
        if status_str != "ready":
            counts["not_ready"] += 1
            continue

        # 🔍 Validar datos mínimos requeridos
        reason, vals = _reason_for_skip(sheet, row)
        if reason:
            print(f"⛔ Fila {r} descartada: {reason} | vals={vals}")
            counts["skipped"] += 1
            continue

        # Ahora sí puedes usar vals
        planting_year = vals["plantingyear"]
        trees_contract = vals["treescontract"]
        contract_name = sheet.read(row, "contractname")
        region_sheet = sheet.read(row, "region")
        farmer_number_in_sheet = str(sheet.read(row, "farmernumber") or "").strip()

        # === Snapshot de FPI si viene farmer existente ===
        with engine.begin() as conn:
            existing_cfi = _fetch_personal_snapshot_by_farmer(conn,
                                                              farmer_number_in_sheet) if farmer_number_in_sheet else None

        region = region_sheet
        if _is_blank(region):
            print(f"⛔ Fila {r} descartada: region requerida (solo del sheet).")
            counts["skipped"] += 1
            continue

        # === (A) Asigna contract_code con helper centralizado =====================
        pfx = get_prefix(region)
        if not pfx:
            print(f"⛔ Fila {r} descartada: prefijo inválido para region='{region}'.")
            counts["skipped"] += 1
            continue

        if pfx not in counters:
            counters[pfx] = fetch_max_contract_seq(pfx)  # ← helper toolbox
        counters[pfx] += 1
        contract_code = f"{pfx}{counters[pfx]:04d}"
        print(f"🆕 Fila {r}: asignado contract_code={contract_code}")
        if not dry_run:
            sheet.get_cell(r, cc_idx).value = contract_code

        # ... resto del código ...

        # === (B) Prepara FPI params (nuevo vs clonado) ===========================
        if existing_cfi is None:
            # NUEVO: asigna farmer_number secuencial por región (helper)
            new_farmer_number = fetch_next_farmer_number(region)   # ← helper toolbox
            print(f"🆕 Asignado farmer_number={new_farmer_number} para región={region}")

            fpi_params = extract_group_params(row, sheet.headers, sheet.hdr_df, "fpi", transforms=CFI_XFORM)
            fpi_params["farmer_number"] = new_farmer_number

            if _is_blank(fpi_params.get("contract_name")) and not _is_blank(contract_name):
                fpi_params["contract_name"] = contract_name

            farmer_number_in_sheet = str(new_farmer_number)
        else:
            # CLONADO: arma params 1:1 desde snapshot + override de contract_name del sheet si vino
            fpi_params = {
                "representative":   existing_cfi.get("representative"),
                "farmer_number":    existing_cfi.get("farmer_number"),
                "phone":            existing_cfi.get("phone"),
                "email":            existing_cfi.get("email"),
                "address":          existing_cfi.get("address"),
                "shipping_address": existing_cfi.get("shipping_address"),
                "contract_name":    (contract_name if not _is_blank(contract_name) else existing_cfi.get("contract_name")),
            }

        # === (C) CTI params =======================================================
        cti_params = extract_group_params(row, sheet.headers, sheet.hdr_df, "cti", transforms=CTI_XFORM)
        py_num = _to_int(planting_year)
        cti_params["planting_year"]   = py_num
        cti_params["etp_year"]        = py_num
        cti_params["harvest_year_10"] = (py_num + 10) if py_num is not None else None

        if py_num is not None:
            years_touched.add(py_num)

        if _is_blank(cti_params.get("status")):
            cti_params["status"] = "Pending"

        # === (D) Ejecuta altas usando helpers (y tu marca Done + changelog) ======
        if dry_run:
            to_fpi_preview.append({
                "farmer_number": farmer_number_in_sheet or "(sin FN)",
                "will_append_code": contract_code,
                "representative": fpi_params.get("representative"),
                "phone": fpi_params.get("phone"),
                "email": fpi_params.get("email"),
                "address": fpi_params.get("address"),
                "shipping_address": fpi_params.get("shipping_address"),
            })
            to_cti_preview.append({"contract_code": contract_code, **cti_params})
        else:
            try:
                # FPI alta/append (helper centralizado)
                fpi_insert_or_append(fpi_params, contract_code)

                # CTI alta no-destructiva (helper centralizado)
                cti_insert({
                    "contract_code":      contract_code,
                    "planting_year":      cti_params.get("planting_year"),
                    "etp_year":           cti_params.get("etp_year"),
                    "trees_contract":     cti_params.get("trees_contract"),
                    "planted":            cti_params.get("planted"),
                    "strain":             cti_params.get("strain"),
                    "status":             cti_params.get("status"),
                    "planting_date":      cti_params.get("planting_date"),
                    "species":            cti_params.get("species"),
                    "land_location_gps":  cti_params.get("land_location_gps"),
                    "harvest_year_10":    cti_params.get("harvest_year_10"),
                })

                # 🆕 TRACKEAR CONTRATO CREADO
                contracts_created.append(contract_code)

                sheet.mark_status(r, status_idx, "Done")
                counts["applied"] += 1
                esc = ('nuevo' if existing_cfi is None else 'clonado')
                print(f"✅ Fila {r} aplicada y marcada Done (escenario={esc})")

                # Bridge a ChangeLog
                try:
                    log_new_contract_to_changelog(
                        contract_code=contract_code,
                        scenario=esc,
                        farmer_number=farmer_number_in_sheet or "",
                        source_row=r,
                    )
                except Exception as _e:
                    print(f"⚠️  No se pudo registrar en changelog.xlsx (fila {r}, {contract_code}): {_e}")

            except Exception as e:
                counts["failed"] += 1
                print(f"💥 Fila {r} error: {e}")

    # ========== FIN DEL LOOP ==========

    # 🆕 SINCRONIZAR CONTRATOS CREADOS (ANTES DE GUARDAR)
    if not dry_run and contracts_created:
        print(f"\n🔄 Sincronizando {len(contracts_created)} contratos nuevos en CA...")
        try:
            from core.sync import sync_contract_allocation_full  # 👈 Import aquí si no lo pusiste arriba
            sync_contract_allocation_full(contracts_created)
            print("✅ Sincronización de CA completada")
        except Exception as e:
            print(f"⚠️ Error en sincronización de CA: {e}")

    # === 🧾 Guardar cambios en el catálogo y hacer backups ===
    if not dry_run and counts["applied"] > 0:
        sheet.save()
        print(f"💾 Archivo {CATALOG_FILE} guardado con {counts['applied']} cambios aplicados.")

        try:
            print("💾 Backup del catálogo de entrada (única versión)…")
            out_cat = rotate_excel_backup_single_latest(
                tag="new_contracts_input",
                pattern_prefix="newcontracts_catalog_bkp"
            )
            shutil.copyfile(CATALOG_FILE, out_cat)
        except Exception as e:
            print(f"⚠️  No se pudo respaldar el catálogo {CATALOG_FILE}: {e}")

        # 🔥 ESTO ES LO QUE FALTABA: EXPORTAR LAS TABLAS ACTUALIZADAS
        try:
            print("💾 Re-escribiendo Excel actualizado (masterdatabase_export.xlsx)…")
            export_tables_to_excel(
                engine,
                [
                    "contract_tree_information",
                    "farmer_personal_information",
                    "contract_allocation",
                    "inventory_metrics",
                    "inventory_metrics_current",
                ],
                out_path=EXCEL_FILE,
            )

            print("💾 Backup del export final (única versión)…")
            out_xlsx = rotate_excel_backup_single_latest(
                tag="new_contracts_input",
                pattern_prefix="newcontracts_export_bkp"
            )
            shutil.copyfile(EXCEL_FILE, out_xlsx)
            print(f"✅ Export guardado y respaldado: {EXCEL_FILE}")

        except Exception as e:
            print(f"⚠️  Falló export_tables_to_excel o su backup: {e}")

    # === Previews en dry_run ===
    if dry_run:
        if to_fpi_preview:
            print("\n=== Preview FPI ===")
            print(pd.DataFrame(to_fpi_preview))
        if to_cti_preview:
            print("\n=== Preview CTI (top 3) ===")
            print(pd.DataFrame(to_cti_preview).head(3))
        if to_ca_preview:
            print("\n=== Preview CA ===")
            print(pd.DataFrame(to_ca_preview))
        else:
            print("\n🪵 No hay preview CA: ninguna fila califica para backfill.")

    print("\n" + "="*60)
    print("✅ Contract Code serializado por prefijo y orden de fila.")
    print(f"✅ Done: {counts['applied']} | ❌ Failed: {counts['failed']} | ⏸️ Not ready: {counts['not_ready']} | ⏭️ Skipped: {counts['skipped']}")
    print(f"🔧 Modo: {'DRY RUN' if dry_run else 'PRODUCCIÓN'}")
    print("="*60)


# ⚡⚡⚡ ESTO ES LO QUE FALTABA ⚡⚡⚡
if __name__ == "__main__":
    import sys
    dry_run = "--dry-run" in sys.argv
    print(f"🚀 Iniciando procesamiento de nuevos contratos (dry_run={dry_run})...")
    main(dry_run=dry_run)
    print("🏁 Proceso completado.")

