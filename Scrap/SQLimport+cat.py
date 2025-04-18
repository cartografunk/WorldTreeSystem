import pandas as pd
from sqlalchemy import create_engine, text


# =============================================================================
# Función para parsear el valor catálogo.
# Dado un string del tipo "1) Live", extrae el id y el texto en inglés.
# Devuelve una tupla (id, texto_en_inglés). Si falla, regresa (None, None).
# =============================================================================
def parse_catalog_value(val: str):
    if pd.isna(val) or ')' not in val:
        return None, None
    try:
        parts = val.split(')', 1)
        cat_id = int(parts[0].strip())
        cat_name_en = parts[1].strip()
        return cat_id, cat_name_en
    except Exception as e:
        print(f"Error al parsear el valor '{val}': {e}")
        return None, None


# =============================================================================
# Parámetros y conexión a la base de datos
# =============================================================================

# Ruta del archivo CSV (ajusta la ruta según corresponda)
csv_path = r"C:\Users\HeyCe\World Tree Technologies Inc\Forest Inventory - Documentos\USA\2024_ForestInventoryQ1_25\WT Cruises\Combined_Inventory.csv"

# Crear la conexión a PostgreSQL
engine = create_engine("postgresql+psycopg2://postgres:pauwlonia@localhost:5432/gisdb")

# Lee el CSV en un DataFrame
df = pd.read_csv(csv_path)

# =============================================================================
# Definición de columnas catálogo y su tabla correspondiente en la base de datos.
# =============================================================================
catalog_columns = {
    'Species': 'cat_species',
    'Defect': 'cat_defect',
    'Pests': 'cat_pest',
    'Disease': 'cat_disease',
    'Coppiced': 'cat_coppiced',
    'Permanent Plot': 'cat_permanent_plot',
    'Status': 'cat_status'
}

# =============================================================================
# Previo al procesamiento, aseguramos que cada tabla catálogo tenga la columna
# "nombre_en". Se utiliza "ADD COLUMN IF NOT EXISTS" (disponible en PostgreSQL >= 9.6).
# =============================================================================
with engine.begin() as conn:
    for csv_col, cat_table in catalog_columns.items():
        alter_query = text(f"ALTER TABLE {cat_table} ADD COLUMN IF NOT EXISTS nombre_en TEXT;")
        conn.execute(alter_query)
        print(f"Verificado/Creado columna 'nombre_en' en la tabla '{cat_table}'.")

# =============================================================================
# Procesamiento: Para cada columna catálogo, extraemos los valores únicos, los parseamos,
# y actualizamos la columna 'nombre_en' en la tabla de catálogo correspondiente.
# =============================================================================
with engine.begin() as conn:
    for csv_col, cat_table in catalog_columns.items():
        print(f"\nProcesando columna '{csv_col}' para la tabla '{cat_table}':")

        # Extraer valores únicos no nulos de la columna del DataFrame
        unique_vals = df[csv_col].dropna().unique()
        mapping = {}

        for val in unique_vals:
            cat_id, cat_name_en = parse_catalog_value(val)
            if cat_id is not None and cat_name_en:
                mapping[cat_id] = cat_name_en

        print(f"  Valores encontrados: {mapping}")

        for cat_id, english_label in mapping.items():
            query = text(f"""
                UPDATE {cat_table}
                   SET nombre_en = :english_label
                 WHERE id = :cat_id
            """)
            conn.execute(query, {"english_label": english_label, "cat_id": cat_id})
            print(f"    Actualizado id {cat_id} -> '{english_label}' en '{cat_table}'")

    print("\nActualización de catálogos completada.")
