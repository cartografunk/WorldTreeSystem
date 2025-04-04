import pandas as pd
from sqlalchemy import create_engine
import re

# Configuración de conexión
DB_NAME = "gisdb"
DB_USER = "postgres"
DB_PASSWORD = "pauwlonia"
DB_HOST = "localhost"
DB_PORT = "5432"

excel_path = r"C:\Users\HeyCe\World Tree Technologies Inc\Forest Inventory - Documentos\Inventory_DataBase_Processed\CR_InventoryDataBase_Q1_2025.xlsm"
main_sheet = "Database"
output_table = "cr_inventory_2025"

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# --- 1. Cargar hoja principal ---
df = pd.read_excel(excel_path, sheet_name=main_sheet, engine="openpyxl")

# --- 2. Omitir campos que se calculan o se derivan de catálogos ---
omitidos = ["Doyle BF", "Alive Tree", "Dead Trees"]
df = df.drop(columns=[c for c in omitidos if c in df.columns])

# --- 3. Cargar y reemplazar STATUS con status_id ---
cat_status = pd.read_sql("SELECT * FROM cat_status", con=engine)
df = df.merge(cat_status[['id_Status', 'Status']], how="left", on="Status")
df = df.drop(columns=["Status"])
df = df.rename(columns={"id_Status": "status_id"})

# --- 4. Función para extraer ID y nombre del formato "1) Texto" ---
def separar_id_valor(valor):
    if pd.isnull(valor):
        return None, None
    match = re.match(r"^\s*(\d+)\)\s*(.+)$", str(valor))
    if match:
        return int(match.group(1)), match.group(2).strip()
    else:
        return None, str(valor).strip()

# --- 5. Procesar catálogos adicionales ---
campos_catalogo = {
    "Species": "cat_species",
    "Defect": "cat_defect",
    "Pest": "cat_pest",
    "Coppiced": "cat_coppiced",
    "Permanent Plot": "cat_permanent_plot"
}

for col, catalogo in campos_catalogo.items():
    # Extraer id y valor
    temp = df[[col]].dropna().drop_duplicates().copy()
    temp[['id', 'nombre']] = temp[col].apply(lambda x: pd.Series(separar_id_valor(x)))
    temp = temp.dropna(subset=['id'])
    temp = temp.drop_duplicates(subset=['id'])
    temp = temp.sort_values('id')

    # Guardar catálogo
    temp.to_sql(name=catalogo, con=engine, if_exists='replace', index=False)
    print(f"✅ Catálogo creado: {catalogo}")

    # Hacer merge para obtener id
    df = df.merge(temp[['id', col]], how='left', on=col)
    df = df.drop(columns=[col])
    df = df.rename(columns={"id": f"{catalogo}_id"})

# --- 6. Calcular Doyle BF ---
df["DBH (in)"] = pd.to_numeric(df["DBH (in)"], errors="coerce")
df["THT (ft)"] = pd.to_numeric(df["THT (ft)"], errors="coerce")
df["doyle_bf"] = ((df["DBH (in)"] - 4) ** 2) * (df["THT (ft)"] / 16)

# --- 7. Guardar tabla final ---
df.to_sql(name=output_table, con=engine, if_exists='replace', index=False)
print(f"✅ Datos cargados exitosamente a {output_table}")
