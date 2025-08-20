from pathlib import Path
from sqlalchemy import text
from core.libs import pd
from core.db import get_engine
from core.paths import DATABASE_EXPORTS_DIR, ensure_all_paths_exist
from core.backup import backup_excel, backup_tables
from core.sheets import Sheet, read_changelog_catalogs, get_table_for_field, export_tables_to_excel

EXCEL_FILE = Path(DATABASE_EXPORTS_DIR) / "masterdatabase_export.xlsx"
CATALOG_FILE = Path(DATABASE_EXPORTS_DIR) / "changelog.xlsx"
SHEET_NAME = "ChangeLog"

ensure_all_paths_exist()


def _is_ready(v) -> bool:
    return bool(v) and str(v).strip().lower() == "ready"


def process_changelog_and_update_sql(engine, fields_catalog: pd.DataFrame):
    if engine is None:
        engine = get_engine()

    sheet = Sheet(CATALOG_FILE, SHEET_NAME)

    code_col   = sheet.index_of("contract_code")
    field_col  = sheet.index_of("target_field")
    change_col = sheet.index_of("change")
    status_col = sheet.index_of("change_in_db")  # ← usamos change_in_db
    if not all([code_col, field_col, change_col, status_col]):
        raise RuntimeError("❌ Falta alguna columna en ChangeLog (contract_code, target_field, change, change_in_db).")

    changes_applied = 0
    for r, row in sheet.iter_rows():
        status_val = sheet.get_cell(r, status_col).value
        # Solo procesa si está EXACTAMENTE "Ready" (case-insensitive)
        if not _is_ready(status_val):
            continue

        contract_code = sheet.get_cell(r, code_col).value
        target_field  = sheet.get_cell(r, field_col).value
        change        = sheet.get_cell(r, change_col).value
        if not contract_code or not target_field:
            continue

        table = get_table_for_field(fields_catalog, target_field)
        if not table:
            print(f"❌ Campo '{target_field}' no encontrado en FieldsCatalog")
            continue

        stmt = text(f'''
            UPDATE masterdatabase."{table}"
            SET "{target_field}" = :val
            WHERE contract_code = :cc
        ''')

        with engine.begin() as conn:
            conn.execute(stmt, {"val": change, "cc": str(contract_code)})

        # ✅ Si llegó aquí, lo marcamos como Done
        sheet.mark_done(r, status_col, "Done")
        changes_applied += 1

    sheet.save()
    print(f"✅ Cambios aplicados: {changes_applied}. (Se procesaron solo filas con change_in_db='Ready')")


def main():
    engine = get_engine()

    # Backups centralizados (archivos)
    print("💾 Backup Excel export...")
    backup_excel(EXCEL_FILE)
    print("💾 Backup changelog...")
    backup_excel(CATALOG_FILE)

    print("📚 Leyendo catálogos...")
    fields_catalog, _ = read_changelog_catalogs(CATALOG_FILE)

    # Backups centralizados (tablas que puede tocar el changelog)
    try:
        candidate_tables = sorted(
            t for t in fields_catalog["target_table"].dropna().astype(str).unique()
        )
        if candidate_tables:
            print(f"🛡️  Backup tablas antes de aplicar cambios: {candidate_tables}")
            backup_tables(engine, candidate_tables, schema="masterdatabase", label="pre_changelog")
    except Exception as e:
        print(f"⚠️  No se pudieron determinar tablas a respaldar: {e}")

    print("🚩 Aplicando cambios pendientes (solo 'Ready')...")
    process_changelog_and_update_sql(engine, fields_catalog)

    print("💾 Re-escribiendo Excel actualizado...")
    export_tables_to_excel(engine, [
        "contract_tree_information",
        "contract_farmer_information",
        "contract_allocation",
        "inventory_metrics",
        "inventory_metrics_current",
    ], out_path=EXCEL_FILE)

    print("🏁 Flujo completo terminado.")


if __name__ == "__main__":
    main()
