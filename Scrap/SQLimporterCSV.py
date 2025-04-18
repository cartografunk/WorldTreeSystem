import pandas as pd
from sqlalchemy import create_engine

# -----------------------------------------------------------------------------
# Función para parsear valores de catálogo.
# Esta versión remueve cualquier punto final en el ID extraído.
# Por ejemplo, dada la cadena "1.) Yes", retorna la tupla (1, "Yes").
# -----------------------------------------------------------------------------
def parse_catalog_value(val: str):
    if pd.isna(val) or ')' not in str(val):
        return None, None
    try:
        # Separamos por la primera ocurrencia de ')'
        parts = val.split(')', 1)
        # Limpiamos la parte que debería ser el ID: quitamos espacios y, si existe, puntos al final.
        id_str = parts[0].strip().rstrip('.')
        cat_id = int(id_str)
        # La parte restante es el texto en inglés
        cat_name_en = parts[1].strip()
        return cat_id, cat_name_en
    except Exception as e:
        print(f"Error al parsear el valor '{val}': {e}")
        return None, None

# -----------------------------------------------------------------------------
# Función para obtener solamente el ID del catálogo a partir del valor.
# -----------------------------------------------------------------------------
def convert_catalog_id(x):
    if pd.isnull(x):
        return None
    x_str = str(x)
    if ')' in x_str:
        cat_id, _ = parse_catalog_value(x_str)
        return cat_id
    return x

# -----------------------------------------------------------------------------
# Definición de las columnas de catálogo y sus tablas asociadas.
# Estas son las columnas en el CSV que hacen referencia a catálogos.
# -----------------------------------------------------------------------------
catalog_columns = {
    'Species': 'cat_species',
    'Defect': 'cat_defect',
    'Pests': 'cat_pest',
    'Disease': 'cat_disease',
    'Coppiced': 'cat_coppiced',
    'Permanent Plot': 'cat_permanent_plot',
    'Status': 'cat_status'
}

# -----------------------------------------------------------------------------
# Lectura del CSV
# -----------------------------------------------------------------------------
csv_path = r"C:\Users\HeyCe\World Tree Technologies Inc\Forest Inventory - Documentos\USA\2024_ForestInventoryQ1_25\WT Cruises\Combined_Inventory.csv"
df = pd.read_csv(csv_path)

# -----------------------------------------------------------------------------
# Transformación de los valores en cada columna catálogo:
# Se reemplaza el valor original (por ejemplo, "1.) Yes") por el ID numérico.
# -----------------------------------------------------------------------------
for col in catalog_columns.keys():
    if col in df.columns:
        df[col] = df[col].apply(convert_catalog_id)
        print(f"Columna '{col}' convertida. Valores únicos resultantes: {df[col].dropna().unique()}")

# -----------------------------------------------------------------------------
# Conexión a la base de datos PostgreSQL.
# Ajusta la cadena de conexión según tu usuario, contraseña, host, puerto y base de datos.
# -----------------------------------------------------------------------------
engine = create_engine("postgresql+psycopg2://postgres:pauwlonia@localhost:5432/gisdb")

# -----------------------------------------------------------------------------
# Importar el DataFrame a PostgreSQL en la tabla 'inventory_us_2025'.
# Se creará la tabla automáticamente (si ya existe, se reemplazará).
# -----------------------------------------------------------------------------
df.to_sql('inventory_us_2025', engine, if_exists='replace', index=False)
print("La importación a la tabla 'inventory_us_2025' ha finalizado exitosamente.")
