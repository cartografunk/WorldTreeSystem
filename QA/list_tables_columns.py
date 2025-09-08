# QA/list_tables_columns.py
from core.db import get_engine
import pandas as pd

engine = get_engine()

q = """
SELECT
    table_schema,
    table_name,
    column_name,
    data_type
FROM information_schema.columns
WHERE table_schema NOT IN ('pg_catalog','information_schema')
ORDER BY table_schema, table_name, ordinal_position;
"""

df = pd.read_sql(q, engine)

# Export a Excel para revisarlo cómodo
out_xlsx = "C:/Users/HeyCe/World Tree Technologies Inc/Operations - Documentos/WorldTreeSystem/QA/tables_and_columns.xlsx"
df.to_excel(out_xlsx, index=False)

print("✅ Exportado listado de tablas y columnas a:")
print('"'+out_xlsx+'"')