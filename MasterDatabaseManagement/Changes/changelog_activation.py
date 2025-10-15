#MasterDatabaseManagement\Changes\changelog_activation.py
from core.libs import pd, text, Path
from core.db import get_engine
from core.paths import DATABASE_EXPORTS_DIR, ensure_all_paths_exist
#from core.backup import backup_excel, backup_tables
from core.backup_control import (
    backup_once,
    rotate_excel_backup_single_latest,   # ← usar para Excel “única versión”
)
from core.sheets import Sheet, read_changelog_catalogs, get_table_for_field, export_tables_to_excel, STATUS_DONE
from MasterDatabaseManagement.Changes.farmer_personal_information import apply_changelog_change
import warnings
warnings.filterwarnings("ignore", message="Data Validation extension is not supported and will be removed")
from MasterDatabaseManagement.sanidad.ca_backfill import upsert_ca_etp_from_cti


EXCEL_FILE = Path(DATABASE_EXPORTS_DIR) / "masterdatabase_export.xlsx"
CATALOG_FILE = Path(DATABASE_EXPORTS_DIR) / "changelog.xlsx"
SHEET_NAME = "ChangeLog"

sheet = Sheet(CATALOG_FILE, SHEET_NAME)
status_col = sheet.ensure_status_column("change_in_db")
code_col   = sheet.index_of("contract_code")
field_col  = sheet.index_of("target_field")
change_col = sheet.index_of("change")

ensure_all_paths_exist()

# --- soporte directo FPI por contract_code ---
ALLOWED_FPI_FIELDS = {
    "representative", "farmer_number", "phone", "email",
    "address", "shipping_address", "contract_name"
}
SQL_FPI_UPDATE_TMPL = """
    UPDATE masterdatabase.farmer_personal_information AS fpi
       SET {column} = :val
     WHERE :cc = ANY(fpi.contract_codes)
"""
def _apply_change_fpi_via_contract(conn, contract_code: str, target_field: str, change_val):
    col = str(target_field).strip()
    if col not in ALLOWED_FPI_FIELDS:
        return False, f"target_field '{col}' no permitido para FPI"

    sql = text(SQL_FPI_UPDATE_TMPL.format(column=col))
    conn.execute(sql, {"cc": str(contract_code), "val": change_val})
    return True, "single"



def _is_ready(v) -> bool:
    return bool(v) and str(v).strip().lower() == "ready"


def process_changelog_and_update_sql(engine, fields_catalog: pd.DataFrame, reasons_df: pd.DataFrame, dry_run: bool = False):
    sheet = Sheet(CATALOG_FILE, SHEET_NAME)

    status_col = sheet.ensure_status_column("change_in_db")
    code_col   = sheet.index_of("contract_code")
    field_col  = sheet.index_of("target_field")
    change_col = sheet.index_of("change")
    reason_col = (
        sheet.index_of("reason_id")
        or sheet.index_of("reason")
        or sheet.index_of("change_reason")
        or sheet.index_of("change_reason_id")
    )

    if not all([status_col, code_col, field_col, change_col]):
        raise RuntimeError("❌ Falta alguna columna en ChangeLog (contract_code, target_field, change, change_in_db).")

    counts = {"ready": 0, "applied": 0, "single": 0, "propagated": 0, "skipped": 0, "failed": 0}

    with engine.begin() as conn:
        for r, row in sheet.iter_ready_rows(status_col):
            counts["ready"] += 1
            contract_code = sheet.get_cell(r, code_col).value
            target_field  = sheet.get_cell(r, field_col).value
            change_val    = sheet.get_cell(r, change_col).value
            reason_val    = sheet.get_cell(r, reason_col).value if reason_col else None

            # Resuelve tabla destino
            table = get_table_for_field(fields_catalog, target_field)
            if not table:
                counts["skipped"] += 1
                print(f"⏭️  Fila {r} omitida: '{target_field}' no está en FieldsCatalog")
                continue

            try:
                try:
                    if dry_run:
                        # Preview: no tocar DB ni Excel
                        print(
                            f"👀 DRY-RUN fila {r}: "
                            f"UPDATE masterdatabase.\"{table}\" SET \"{target_field}\" = {change_val!r} "
                            f"WHERE contract_code = {contract_code!r} (reason={reason_val!r})"
                        )
                        continue

                    # --- 🚩 Si el destino es FPI, aplicamos por pertenencia en contract_codes ---
                    if table == "farmer_personal_information":
                        ok, mode = _apply_change_fpi_via_contract(
                            conn,
                            contract_code=str(contract_code),
                            target_field=str(target_field),
                            change_val=change_val
                        )
                        if ok:
                            sheet.mark_status(r, status_col, STATUS_DONE)
                            counts["applied"] += 1
                            counts["single"] += 1 if mode == "single" else 0
                        else:
                            counts["skipped"] += 1
                            print(f"⏭️  Fila {r} omitida (FPI): {mode}")
                        continue

                    # --- resto de tablas: usa tu flujo existente ---
                    res = apply_changelog_change(
                        conn,
                        contract_code=str(contract_code),
                        target_field=str(target_field),
                        change_val=change_val,
                        reason_val=reason_val,
                        fields_catalog=fields_catalog,
                    )

                    if res.ok:
                        sheet.mark_status(r, status_col, STATUS_DONE)
                        counts["applied"] += 1
                        if res.mode == "propagated":
                            counts["propagated"] += 1
                        elif res.mode == "single":
                            counts["single"] += 1
                        else:
                            counts["skipped"] += 1
                    else:
                        counts["skipped"] += 1
                        print(f"⏭️  Fila {r} omitida: {res.info}")

                except Exception as e:
                    counts["failed"] += 1
                    print(f"💥 Error en fila {r} (cc={contract_code}, field={target_field}): {e}")


            except Exception as e:
                counts["failed"] += 1
                print(f"💥 Error en fila {r} (cc={contract_code}, field={target_field}): {e}")

    if not dry_run:
        sheet.save()

    print(f"✅ ready={counts['ready']} | applied={counts['applied']} | single={counts['single']} | "
          f"propagated={counts['propagated']} | skipped={counts['skipped']} | failed={counts['failed']} | dry_run={dry_run}")


def main(dry_run: bool = False):
    engine = get_engine()

    # 🔒 Backup ÚNICO por tabla/tag (reemplaza el anterior)
    tables_to_backup = [
        ("masterdatabase", "contract_farmer_information"),
        ("masterdatabase", "contract_tree_information"),
    ]
    df_backup_log = backup_once(tables_to_backup, tag="changelog_activation", engine=engine)

    # 🗂️ Backups de Excel “una sola versión” por tag (opcional, solo en vivo)
    if not dry_run:
        try:
            print("💾 Backup Excel (única versión)…")
            out1 = rotate_excel_backup_single_latest(tag="changelog_activation", pattern_prefix="changelog_bkp")
            shutil.copyfile(EXCEL_FILE, out1)

            print("💾 Backup changelog (única versión)…")
            out2 = rotate_excel_backup_single_latest(tag="changelog_activation", pattern_prefix="changelog_catalog_bkp")
            shutil.copyfile(CATALOG_FILE, out2)
        except Exception as e:
            print(f"⚠️  Backup de Excel omitido: {e}")

    print("📚 Leyendo catálogos…")
    fields_catalog, reasons_catalog = read_changelog_catalogs(CATALOG_FILE)

    print(f"🚩 Aplicando cambios pendientes (solo 'Ready')… dry_run={dry_run}")
    process_changelog_and_update_sql(engine, fields_catalog, reasons_catalog, dry_run=dry_run)

    # 🔧 Backfill/UPSERT de CA desde CTI (generalizado >2018)
    if not dry_run:
        res = upsert_ca_etp_from_cti(target_year=None)  # None => etp_year > 2018
        print(f"[ca_backfill] remaining (ca.*_etp aún en 0/NULL): {res['remaining']}")

    # (Opcional) Backup de tablas afectadas, ya con CA alineado
    if not dry_run:
        try:
            candidate_tables = sorted(
                t for t in fields_catalog["target_table"].dropna().astype(str).unique()
            )
            if candidate_tables:
                print(f"🛡️  Backup tablas post cambios: {candidate_tables}")
                backup_tables(engine, candidate_tables, schema="masterdatabase", label="post_changelog")
        except Exception as e:
            print(f"⚠️  Backup tablas omitido: {e}")

    print(f"🚩 Aplicando cambios pendientes (solo 'Ready')... dry_run={dry_run}")
    process_changelog_and_update_sql(engine, fields_catalog, reasons_catalog, dry_run=dry_run)

    # Export a Excel solo en vivo
    if not dry_run:
        print("💾 Re-escribiendo Excel actualizado...")
        export_tables_to_excel(engine, [
            "contract_tree_information",
            "farmer_personal_information",
            "contract_allocation",
            "inventory_metrics",
            "inventory_metrics_current",
        ], out_path=EXCEL_FILE)

    print("🏁 Flujo completo terminado.")


if __name__ == "__main__":
    main()
