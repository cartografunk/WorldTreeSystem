# scripts/upload_timeseries_survival.py

from core.db import get_engine
from core.libs import pd
from sqlalchemy import Float, Integer, Text
from sqlalchemy.sql import text

# Ruta al Excel
EXCEL_PATH = r"C:\Users\HeyCe\World Tree Technologies Inc\Operations - Documentos\Main Database\Reports\Monthly Report 2.0 Framework.xlsx"

# Cargar hoja del timeseries
df_ts = pd.read_excel(EXCEL_PATH, sheet_name="timeseries_survival")

# Limpieza: columnas y survival_pct
df_ts.columns = [col.strip().lower().replace(" ", "_") for col in df_ts.columns]
df_ts['survival_pct'] = df_ts['survival_pct'].astype(str).str.replace('%', '').astype(float) / 100

# Revisar
print("📄 Previsualización:")
print(df_ts.head())

# Subir a PostgreSQL
engine = get_engine()
table_name = "survival_timeseries"
schema = "masterdatabase"

with engine.connect() as conn:
    conn.execute(text(f"DROP TABLE IF EXISTS {schema}.{table_name}"))
    print(f"🧹 Tabla anterior eliminada: {schema}.{table_name}")

df_ts.to_sql(
    table_name,
    con=engine,
    schema=schema,
    if_exists="replace",
    index=False,
    dtype={
        "contract_code": Text(),
        "survival_metric_source": Text(),
        "survival_pct": Float(),
        "survival_count": Integer(),
        "survival_count_us_allocation": Integer(),
    }
)

print(f"✅ Tabla subida: {schema}.{table_name}")
