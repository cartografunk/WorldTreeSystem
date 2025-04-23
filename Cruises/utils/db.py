# utils/db.py

from sqlalchemy import create_engine
import pandas as pd

# Conexión
def get_engine():
    return create_engine("postgresql+psycopg2://postgres:pauwlonia@localhost:5432/helloworldtree")
    print("💻 Conectado a SQL")



def get_table_names(country_code="cr", year="2025", schema="public"):
    """
    Devuelve un diccionario con las tablas clave de inventario para un país y año específico.
    Ejemplo: get_table_names("us", "2025") → tablas US para 2025
    """
    suffix = f"{country_code}_inventory_{year}"
    return {
        "Catálogo Contratos": f"{schema}.cat_{suffix}",
        "Inventario Detalle": f"{schema}.{suffix}"
    }


def inspect_tables(engine, table_dict):
    """
    Inspecciona las tablas pasadas en el diccionario y despliega estadísticas básicas.
    """
    for nombre_tabla, sql_tabla in table_dict.items():
        print(f"\n=== {nombre_tabla} ({sql_tabla}) ===")
        try:
            df = pd.read_sql(f'SELECT * FROM {sql_tabla} LIMIT 5', engine)

            print("\n→ Primeros registros:\n", df.to_string(index=False))
            print("\n→ Info de columnas:\n")
            df.info()
            print("\n→ Estadísticas descriptivas:\n")
            print(df.describe(include='all'))

        except Exception as e:
            print(f"⚠️ Error consultando {sql_tabla}: {e}")
