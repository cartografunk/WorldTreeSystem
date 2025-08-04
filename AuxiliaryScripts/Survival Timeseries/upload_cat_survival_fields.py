# scripts/upload_cat_survival_fields.py

from core.db import get_engine
from core.libs import pd, Text, Integer, text

# Ruta al Excel
EXCEL_PATH = r"C:\Users\HeyCe\World Tree Technologies Inc\Operations - Documentos\Main Database\Reports\Monthly Report 2.0 Framework.xlsx"

# Cargar hoja del catálogo
df_cat = pd.read_excel(EXCEL_PATH, sheet_name="cat_survival_fields")

# Limpiar columnas (por si Excel exportó raro)
df_cat.columns = [col.strip().lower().replace(" ", "_") for col in df_cat.columns]

# Verificar contenido
print("📄 Previsualización:")
print(df_cat)

# Subir a PostgreSQL (en esquema public)
engine = get_engine()
table_name = "cat_survival_fields"
schema = "public"

with engine.connect() as conn:
    conn.execute(text(f"DROP TABLE IF EXISTS {schema}.{table_name}"))
    print(f"🧹 Tabla anterior eliminada: {schema}.{table_name}")

df_cat.to_sql(
    table_name,
    con=engine,
    schema=schema,
    if_exists="replace",
    index=False,
    dtype = {
        "id": Integer(),
        "survival_metric_source": Text(),
        "priority": Integer(),
    }

)

print(f"✅ Tabla subida: {schema}.{table_name}")
