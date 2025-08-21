# MasterDatabaseManagement/Exports/masterdatabase_to_Excel.py

from pathlib import Path
from core.db import get_engine
from core.libs import pd
from core.paths import DATABASE_EXPORTS_DIR, ensure_all_paths_exist

engine = get_engine()
ensure_all_paths_exist()  # asegura que exista la carpeta de exports

query = """
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'masterdatabase'
ORDER BY table_name;
"""
tables = pd.read_sql(query, engine)["table_name"].tolist()

def remove_tz(df: pd.DataFrame) -> pd.DataFrame:
    # Quita tz de columnas datetime (robusto)
    for col in df.columns:
        if pd.api.types.is_datetime64tz_dtype(df[col]):
            df[col] = df[col].dt.tz_localize(None)
    return df

outfile = Path(DATABASE_EXPORTS_DIR) / "masterdatabase_export.xlsx"

with pd.ExcelWriter(outfile, engine="openpyxl") as writer:
    for table in tables:
        print(f"Exportando {table} ...")
        df = pd.read_sql(f'SELECT * FROM masterdatabase."{table}"', engine)
        if not df.empty:
            df = remove_tz(df)
            # ordena por la primera columna si es posible
            try:
                df = df.sort_values(by=df.columns[0])
            except Exception:
                pass
        df.to_excel(writer, sheet_name=table[:31], index=False)

print(f"✅ Todas las tablas exportadas a {outfile}")
