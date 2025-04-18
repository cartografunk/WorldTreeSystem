import pandas as pd
from sqlalchemy import create_engine, text

# — Configuración —
conn_string = "postgresql+psycopg2://postgres:pauwlonia@localhost:5432/gisdb"
table_name = "US_InventoryDatabase_Q1_2025b"
engine = create_engine(conn_string)

# — Lista de columnas a eliminar —
columns_to_drop = [
    "Plot Coordinate_Corrected",
    "Correction_Date"
]

with engine.begin() as conn:
    # Verificar y eliminar cada columna
    for column in columns_to_drop:
        try:
            # Verificar si la columna existe
            column_exists = conn.execute(text(
                f"""SELECT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = '{table_name.lower()}' 
                    AND column_name = '{column.lower()}'
                )"""
            )).scalar()

            if column_exists:
                conn.execute(text(f'ALTER TABLE "{table_name}" DROP COLUMN "{column}"'))
                print(f"✅ Columna '{column}' eliminada correctamente")
            else:
                print(f"⚠️ La columna '{column}' no existe en la tabla")

        except Exception as e:
            print(f"❌ Error al eliminar columna '{column}': {str(e)}")

print("Proceso completado")