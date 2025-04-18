# forest_inventory/sql_importer.py

from utils.libs import pd
from utils.db import get_engine

def import_catalog_to_sql(df, table_name, connection_string, if_exists="append", schema=None):
    """
    Importa un catálogo a SQL desde un DataFrame.

    Args:
        df (pd.DataFrame): Catálogo a importar.
        table_name (str): Nombre de la tabla destino (ej. 'cat_species').
        connection_string (str): Cadena SQLAlchemy.
        if_exists (str): 'append', 'replace' o 'fail'.
        schema (str, opcional): Esquema SQL.
    """
    if df.empty:
        print(f"⚠️ DataFrame vacío. No se importó '{table_name}'.")
        return

    engine = get_engine(connection_string)

    df.to_sql(
        name=table_name,
        con=engine,
        if_exists=if_exists,
        index=False,
        schema=schema,
        dtype={
            "ContractCode": sa.VARCHAR(50),  # Ajustar según tu schema
            "CruiseDate": sa.Date()
        }
    )

    print(f"📥 Catálogo importado a tabla '{table_name}' (modo: {if_exists})")
