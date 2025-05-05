# forest_inventory/inventory_importer.py
from utils.libs import pd, unicodedata, re, inspect
from utils.db import get_engine

from sqlalchemy import text

from sqlalchemy import text, inspect

def ensure_table(df, engine, table_name, recreate=False):
    insp = inspect(engine)

    with engine.begin() as conn:
        # 🔧 Quitar columnas duplicadas ANTES de crear tabla
        df = df.loc[:, ~df.columns.duplicated()]

        if recreate or not insp.has_table(table_name):
            if insp.has_table(table_name):
                conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))

            df.head(0).to_sql(table_name, conn, index=False, if_exists="replace")

            # sólo añadimos PK si existe la columna id
            if 'id' in df.columns:
                conn.execute(text(
                    f'ALTER TABLE "{table_name}" '
                    f'ADD CONSTRAINT {table_name}_pk PRIMARY KEY (id)'
                ))
        else:
            existing_cols = {c['name'] for c in insp.get_columns(table_name)}
            for col in df.columns:
                if col not in existing_cols:
                    conn.execute(text(
                        f'ALTER TABLE "{table_name}" ADD COLUMN "{col}" TEXT'
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

        conn.commit()  # guardamos el DDL

        cols = df.columns.tolist()
        cols_quoted = ", ".join([f'"{c}"' for c in cols])
        placeholders = ", ".join(["%s"] * len(cols))

        table_full = f'{schema + "." if schema else ""}"{table_name}"'

        from sqlalchemy import inspect, text
        insp = inspect(engine)
        existing = [c["name"] for c in insp.get_columns(table_name, schema=schema)]

        conflict = ' ON CONFLICT (id) DO NOTHING' if 'id' in existing else ''
        insert_query = (
            f'INSERT INTO {table_full} ({cols_quoted}) '
            f'VALUES ({placeholders})'
            f'{conflict}'
            )

        data = df.values.tolist()

        # Después de definir `insert_query` y antes de iterar batches:
        #print("➤ Columnas que voy a insertar:", cols_quoted)
        # Para inspeccionar el esquema real en la BD:
        from sqlalchemy import inspect
        insp = inspect(engine)
        table_cols = [col["name"] for col in insp.get_columns(table_name)]
        #print("➤ Columnas existentes en la tabla:", table_cols)

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

        print(f"✅ Bulk insert completado: \n '{table_name}' ({len(data)} filas)")
    except Exception as e:
        print(f"❌ Error al realizar bulk insert: \n {str(e)}")

        raise
