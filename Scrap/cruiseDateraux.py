import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# Cargar Excel con los pendientes
df = pd.read_excel(r"C:\Users\HeyCe\World Tree Technologies Inc\Forest Inventory - Documentos\USA\2024_ForestInventoryQ1_25\x Scrap\cruiseStartDate2025_faultyones.xlsx")

# Renombrar columnas si es necesario
df.columns = [c.strip().lower() for c in df.columns]
if "startdate" in df.columns:
    df.rename(columns={"startdate": "cruise_start_date"}, inplace=True)
if "contractcode" not in df.columns:
    raise ValueError("Falta la columna ContractCode en el Excel.")

# Convertir a lista de tuplas
data = list(df[["cruise_start_date", "contractcode"]].itertuples(index=False, name=None))

# Conexión a PostgreSQL
conn = psycopg2.connect(
    dbname="gisdb",
    user="postgres",
    password="pauwlonia",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

# Actualizar los datos
update_query = """
    UPDATE public.cat_inventory_us_2025 AS ci
    SET cruise_start_date = data.cruise_start_date
    FROM (VALUES %s) AS data (cruise_start_date, contract_code)
    WHERE ci."ContractCode" = data.contract_code;
"""
execute_values(cursor, update_query, data)
conn.commit()

print(f"✅ Se actualizaron {len(data)} registros desde el archivo de Excel.")

cursor.close()
conn.close()
