#DatabaseExports/changelog_activation.py

from core.libs import pd, openpyxl, shutil, text
from core.db import get_engine

# Rutas de los archivos
EXCEL_FILE = r"C:\Users\HeyCe\World Tree Technologies Inc\Operations - Documentos\WorldTreeSystem\DatabaseExports\masterdatabase_export.xlsx"
CATALOG_FILE = r"C:\Users\HeyCe\World Tree Technologies Inc\Operations - Documentos\WorldTreeSystem\DatabaseExports\changelog_catalogs.xlsx"
EXCEL_BACKUP = EXCEL_FILE.replace(".xlsx", "_backup.xlsx")

engine = get_engine()

def backup_excel():
    shutil.copyfile(EXCEL_FILE, EXCEL_BACKUP)
    print(f"📝 Backup creado: {EXCEL_BACKUP}")

def read_catalogs():
    fields = pd.read_excel(CATALOG_FILE, sheet_name="FieldsCatalog")
    reasons = pd.read_excel(CATALOG_FILE, sheet_name="ChangeReasonsCatalog")
    return fields, reasons

def read_changelog():
    return pd.read_excel(EXCEL_FILE, sheet_name="ChangeLog")

def get_table_for_field(fields_catalog, field):
    """Devuelve la tabla asociada a un campo usando el catálogo."""
    matches = fields_catalog[fields_catalog['target_field'] == field]
    if not matches.empty:
        return matches['target_table'].iloc[0]
    return None

def apply_pending_changes(df_changelog, fields_catalog, engine):
    # Filtra sólo cambios pendientes (change_in_db vacío o null)
    pending = df_changelog[df_changelog['change_in_db'].isnull() | (df_changelog['change_in_db'] == '')]
    for idx, row in pending.iterrows():
        contract_code = row['contract_code']
        target_field = row['target_field']
        change = row['change']
        table = get_table_for_field(fields_catalog, target_field)
        if not table:
            print(f"❌ Campo '{target_field}' no encontrado en FieldsCatalog")
            continue

        # Detecta si es numérico (puedes hacerlo mejor, esto es básico)
        if change.replace('.', '', 1).isdigit():
            change_value = change
        else:
            change_value = f"'{change}'"

        sql = f"""
        UPDATE masterdatabase.{table}
        SET {target_field} = {change_value}
        WHERE contract_code = '{contract_code}'
        """
        print("Ejecutando:", sql)
        with engine.begin() as conn:
            conn.execute(text(sql))
        # Marca como hecho
        df_changelog["change_in_db"] = df_changelog["change_in_db"].astype(str)
    return df_changelog


def remove_tz(df):
    for col in df.select_dtypes(include=["datetimetz"]).columns:
        df[col] = df[col].dt.tz_localize(None)
    return df

def refresh_tables_in_excel(df_changelog, ordered_tables):
    """Sobrescribe todas las hojas excepto ChangeLog, según el orden oficial."""
    print("⏳ Sobrescribiendo Excel (excepto ChangeLog)...")
    with pd.ExcelWriter(EXCEL_FILE, engine="openpyxl", mode="w") as writer:
        # Escribe ChangeLog primero
        df_changelog.to_excel(writer, sheet_name="ChangeLog", index=False)
        # Escribe el resto, en orden
        for table in ordered_tables[1:]:
            df = pd.read_sql(f'SELECT * FROM masterdatabase."{table}"', engine)
            df = remove_tz(df)
            # Orden opcional: por contract_code si existe
            if "contract_code" in df.columns:
                df = df.sort_values("contract_code")
            df.to_excel(writer, sheet_name=table[:31], index=False)
            print(f"Exportado: {table}")

    print(f"✅ Exportación finalizada: {EXCEL_FILE}")

def main():
    ORDERED_TABLES = [
        "ChangeLog",
        "contract_tree_information",
        "contract_farmer_information",
        "contract_allocation",
        "inventory_metrics",
        "inventory_metrics_current"
    ]
    print("💾 Realizando backup del Excel original...")
    backup_excel()

    print("📚 Leyendo catálogos...")
    fields_catalog, _ = read_catalogs()

    print("🔄 Leyendo ChangeLog...")
    df_changelog = read_changelog()

    print("🚩 Aplicando cambios pendientes...")
    df_changelog = apply_pending_changes(df_changelog, fields_catalog, engine)

    print("💾 Re-escribiendo Excel actualizado...")
    refresh_tables_in_excel(df_changelog, ORDERED_TABLES)

    print("🏁 Flujo completo terminado.")

if __name__ == "__main__":
    main()
