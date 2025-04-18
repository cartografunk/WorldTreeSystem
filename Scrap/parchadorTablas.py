from sqlalchemy import create_engine, inspect
import pandas as pd

# Parámetros
conn_string = "postgresql+psycopg2://postgres:pauwlonia@localhost:5432/gisdb"
table_name = "US_InventoryDatabase_Q1_2025b"

# Crear motor de conexión
engine = create_engine(conn_string)

# Leer los datos antiguos que necesitas parchar
df_old = pd.read_sql(
    "SELECT * FROM public.inventory_us_2025 WHERE \"ContractCode\" IN ('US0164', 'US0024')",
    engine
)

# Renombrar campos si es necesario
df_old.rename(columns={
    "Plot Coordinate": "Plot_Coordinate"
}, inplace=True)

# Inspeccionar columnas de la tabla destino
inspector = inspect(engine)
existing_columns = [col["name"] for col in inspector.get_columns(table_name)]

# Filtrar columnas que coinciden
df_patch = df_old[[col for col in df_old.columns if col in existing_columns]]

# Insertar los registros sin sobrescribir
df_patch.to_sql(
    name=table_name,
    con=engine,
    if_exists="append",
    index=False
)

print(f"✅ Ingresados {len(df_patch)} registros desde inventory_us_2025 a {table_name}")
