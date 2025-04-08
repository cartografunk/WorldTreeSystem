# GeneradordeReportes/utils/db.py
from sqlalchemy import create_engine

def get_engine():
    return create_engine("postgresql+psycopg2://postgres:pauwlonia@localhost:5432/gisdb")

import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)

# Conexión
engine = get_engine()

# Tablas clave
tablas = {
    "Catálogo Contratos": 'public.cat_cr_inventory2025',
    "Inventario Detalle": 'public.cr_inventory_2025'
}

for nombre_tabla, sql_tabla in tablas.items():
    print(f"\n=== {nombre_tabla} ({sql_tabla}) ===")

    try:
        df = pd.read_sql(f'SELECT * FROM {sql_tabla} LIMIT 5', engine)

        print("\n→ Primeros registros:\n")
        print(df.to_string(index=False))

        print("\n→ Info de columnas:\n")
        df.info()

        print("\n→ Estadísticas descriptivas:\n")
        print(df.describe(include='all'))

    except Exception as e:
        print(f"⚠️ Error consultando {sql_tabla}: {e}")