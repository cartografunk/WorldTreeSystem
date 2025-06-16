from core.db import get_engine
from core.schema_helpers import rename_columns_using_schema, get_dtypes_for_dataframe
from core.libs import pd
from sqlalchemy import inspect

def resumen_cruises(schema="public"):
    engine = get_engine()
    insp = inspect(engine)
    tablas = insp.get_table_names(schema=schema)
    tablas_inventory = [t for t in tablas if t.startswith("inventory_")]

    for tabla in tablas_inventory:
        print(f"\n🔍 Procesando {tabla}...")
        df = pd.read_sql_table(tabla, engine, schema=schema)
        # Renombra usando el schema centralizado
        df = rename_columns_using_schema(df)
        # Quitar duplicados
        cat_df = df[["contractcode", "cruisedate"]].drop_duplicates().sort_values(["contractcode", "cruisedate"])
        # Arma el nombre de la tabla destino
        tabla_cat = f"cat_{tabla}"
        # Define dtypes SQL correctos
        dtypes = get_dtypes_for_dataframe(cat_df)
        # Guarda tabla, reemplazando si existe
        cat_df.to_sql(tabla_cat, engine, schema=schema, if_exists="replace", index=False, dtype=dtypes)
        print(f"✅ {tabla_cat} creada con {len(cat_df)} filas únicas.")

if __name__ == "__main__":
    resumen_cruises()
