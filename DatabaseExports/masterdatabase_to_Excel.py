#DatabaseExports/masterdatabase_to_Excel

from core.db import get_engine
from core.libs import pd

engine = get_engine()

query = """
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'masterdatabase'
ORDER BY table_name;
"""
tables = pd.read_sql(query, engine)["table_name"].tolist()

def remove_tz(df):
    for col in df.select_dtypes(include=["datetimetz"]).columns:
        df[col] = df[col].dt.tz_localize(None)
    return df

with pd.ExcelWriter("masterdatabase_export.xlsx", engine="openpyxl") as writer:
    for table in tables:
        print(f"Exportando {table} ...")
        df = pd.read_sql(f'SELECT * FROM masterdatabase."{table}"', engine)
        if not df.empty:
            df = remove_tz(df)
            df = df.sort_values(by=df.columns[0])
        df.to_excel(writer, sheet_name=table[:31], index=False)

print("✅ Todas las tablas exportadas a masterdatabase_export.xlsx")