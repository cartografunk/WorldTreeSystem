import pandas as pd
from sqlalchemy import create_engine

# Parámetros de conexión
DB_NAME = "gisdb"
DB_USER = "postgres"
DB_PASSWORD = "pauwlonia"
DB_HOST = "localhost"
DB_PORT = "5432"

# Ruta al archivo Excel
excel_path = r"C:\Users\HeyCe\World Tree Technologies Inc\Forest Inventory - Documentos\Inventory_DataBase_Processed\CR_InventoryDataBase_Q1_2025.xlsm"

# Crear motor de conexión
engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# 1. Leer el archivo Excel
xlsx = pd.ExcelFile(excel_path, engine="openpyxl")

# 2. Cargar hojas que empiecen con "CAT_"
for sheet_name in xlsx.sheet_names:
    if sheet_name.startswith("cat_"):
        df = xlsx.parse(sheet_name)
        table_name = sheet_name.lower()  # puedes quitar .lower() si prefieres conservar el nombre original

        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='replace',  # reemplaza si ya existe
            index=False
        )
        print(f"📥 Catálogo cargado: {table_name}")
