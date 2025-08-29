from MasterDatabaseManagement.tools.database_management_helpers import remove_tz
from core.db import get_engine
from core.libs import pd, Path
from core.paths import DATABASE_EXPORTS_DIR, ensure_all_paths_exist
from MasterDatabaseManagement.Queries import fetch_active_contracts_query_df

engine = get_engine()
ensure_all_paths_exist()  # asegura que exista la carpeta de exports

# Solo estas tablas, con el nombre de hoja deseado
WANTED = [
    ("contract_tree_information",   "CTI"),
    ("farmer_personal_information", "FPI"),
    ("contract_allocation",         "CA"),
    ("inventory_metrics_current",   "IMC"),
    ("survival_current",            "Survival_Current"),
]

# Qué tablas existen realmente en el esquema
tables = pd.read_sql("""
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'masterdatabase'
""", engine)["table_name"].str.lower().tolist()
existing = set(tables)

outfile = Path(DATABASE_EXPORTS_DIR) / "masterdatabase_export.xlsx"

with pd.ExcelWriter(outfile, engine="openpyxl") as writer:
    # 1) Active Farmers Query al inicio (robusto)
    try:
        afq_df = fetch_active_contracts_query_df(engine)
        if not afq_df.empty:
            afq_df = remove_tz(afq_df)
        afq_df.to_excel(writer, sheet_name="Active Farmers Query", index=False)
        print("Exportando Active Farmers Query -> AFQ")
    except Exception as e:
        pd.DataFrame({"error": [str(e)]}).to_excel(
            writer, sheet_name="Active Farmers Query (error)", index=False
        )

    # 2) Solo las tablas pedidas, en el orden indicado
    for table, sheet_name in WANTED:
        if table.lower() not in existing:
            print(f"⚠️  Tabla no encontrada: {table} (omitida)")
            continue

        print(f"Exportando {table} -> {sheet_name} ...")
        df = pd.read_sql(f'SELECT * FROM masterdatabase."{table}"', engine)
        if not df.empty:
            df = remove_tz(df)
            try:
                df = df.sort_values(by=df.columns[0])
            except Exception:
                pass
        df.to_excel(writer, sheet_name=sheet_name, index=False)

print(f"✅ Export listo: {outfile}")
