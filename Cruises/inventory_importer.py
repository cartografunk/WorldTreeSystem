# forest_inventory/inventory_importer.py
from utils.libs import pd, unicodedata, re, inspect
from utils.db import get_engine

from sqlalchemy import text

from sqlalchemy import text, inspect

def ensure_table(df, engine, table_name, recreate=False):
    insp = inspect(engine)

    with engine.begin() as conn:
        if recreate or not insp.has_table(table_name):
            # DROP si existía y creamos de nuevo con el esquema del DataFrame
            if insp.has_table(table_name):
                conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))

            # cabecera vacía → genera la tabla y todos los tipos
            df.head(0).to_sql(table_name, conn, index=False, if_exists="replace")

            # clave primaria
            conn.execute(text(
                f'ALTER TABLE "{table_name}" '
                f'ADD CONSTRAINT {table_name}_pk PRIMARY KEY (id);'
            ))
        else:
            # tabla ya existe → solo añadimos columnas que falten
            existing_cols = {c['name'] for c in insp.get_columns(table_name)}
            for col in df.columns:
                if col not in existing_cols:
                    conn.execute(text(
                        f'ALTER TABLE "{table_name}" '
                        f'ADD COLUMN "{col}" TEXT'
                    ))



def save_inventory_to_sql(df,
        connection_string,
        table_name,
        if_exists="append",
        schema=None,
        dtype=None,
        progress=False,
        chunksize=1000,
        pre_cleaned=False):
    """Limpia nombres de columnas y guarda el DataFrame en SQL con tipos opcionales."""

    print("\n=== INICIO DE IMPORTACIÓN ===")
    #print("Columnas crudas del archivo:", df.columns.tolist())

    # AQUI: df ya viene renombrado y ordenado por prepare_df_for_sql,
    # así que NO lo tocamos más. Si quieres, mú evita duplicados:
    df = df.loc[:, ~df.columns.duplicated()]

    try:
        engine = get_engine()
        # Bulk insert parametrizado
        conn = engine.raw_connection()
        cursor = conn.cursor()

        table_full = f'{schema + "." if schema else ""}"{table_name}"'

        # 0) Asegurarnos de que existan las columnas metadata
        for col_name, col_type in [("farmername", "TEXT"), ("cruisedate", "DATE")]:
            cursor.execute(
                f'ALTER TABLE {table_full} '
                f'ADD COLUMN IF NOT EXISTS "{col_name}" {col_type};'
            )
        conn.commit()  # guardamos el DDL

        cols = df.columns.tolist()
        cols_quoted = ", ".join([f'"{c}"' for c in cols])
        placeholders = ", ".join(["%s"] * len(cols))

        table_full = f'{schema + "." if schema else ""}"{table_name}"'
        insert_query = (
            f'INSERT INTO {table_full} ({cols_quoted}) VALUES ({placeholders}) '
            f'ON CONFLICT (id) DO NOTHING'
        )

        data = df.values.tolist()

        # Después de definir `insert_query` y antes de iterar batches:
        print("➤ Columnas que voy a insertar:", cols_quoted)
        # Para inspeccionar el esquema real en la BD:
        from sqlalchemy import inspect
        insp = inspect(engine)
        table_cols = [col["name"] for col in insp.get_columns(table_name)]
        print("➤ Columnas existentes en la tabla:", table_cols)

        if progress:
            from tqdm import tqdm  # import ligero, solo si se pide
            iterator = tqdm(
                range(0, len(data), chunksize),
                desc=f"Insertando → {table_name}",
                unit="filas",
                ncols=80
            )
        else:
            iterator = range(0, len(data), chunksize)

        for start in iterator:
            batch = data[start:start + chunksize]
            cursor.executemany(insert_query, batch)

        conn.commit()
        cursor.close()
        conn.close()

        print(f"✅ Bulk insert completado: '{table_name}' ({len(data)} filas)")
    except Exception as e:
        print(f"❌ Error al realizar bulk insert: {str(e)}")

        raise
