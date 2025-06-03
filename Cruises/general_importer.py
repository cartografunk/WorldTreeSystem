#Cruises/general_importer.py
from sqlalchemy import Text, Float, Numeric, SmallInteger, Date
from core.libs import pd, text, inspect, pd
from core.schema import COLUMNS, get_dtypes_for_dataframe, _SA_TO_PD
from core.db import get_engine


# Construye FINAL_ORDER y DTYPES desde schema
SQL_COLUMNS = { col["key"]: col["sql_name"] for col in COLUMNS }
FINAL_ORDER = [ col["sql_name"] for col in COLUMNS ]
DTYPES = {
    col["sql_name"]: col["dtype"]
    for col in COLUMNS
    if "dtype" in col
}

def prepare_df_for_sql(df):
    # 1) renombrar internals → SQL
    df2 = df.rename(columns=SQL_COLUMNS)

    # 2) quitar duplicados
    df2 = df2.loc[:, ~df2.columns.duplicated()]

    # 3) filtrar+reordenar
    cols = [c for c in FINAL_ORDER if c in df2.columns]
    df2 = df2[cols].copy()

    # 4️⃣ Conversión de tipos en base a DTYPES
    for col, dtype in DTYPES.items():
        if col in df2.columns:
            if isinstance(dtype, SmallInteger):
                df2[col] = pd.to_numeric(df2[col], errors='coerce').fillna(0).astype(int)
            elif isinstance(dtype, (Float, Numeric)):
                df2[col] = pd.to_numeric(df2[col], errors='coerce')
            elif isinstance(dtype, Date):
                df2[col] = df2[col].where(df2[col].notna(), None)

    dtype_for_sql = {col: DTYPES[col] for col in df2.columns if col in DTYPES}

    return df2, dtype_for_sql

def create_inventory_catalog(df, engine, table_catalog_name):
    """
    Crea catálogo por ContractCode usando FarmerName desde extractors,
    y completa PlantingYear y TreesContract desde cat_farmers.
    Calcula TreesSampled directamente como el conteo por contrato.
    """
    # --- Paso 1: Validar columnas obligatorias ---
    required_cols = ["contractcode", "farmername", "cruisedate"]
    missing = [col for col in required_cols if col not in df.columns]

    if missing:
        raise KeyError(f"Columnas faltantes: {missing}. Verifica combine_files()")

    # --- Paso 2: Preparar columnas base ---
    cols_base = required_cols.copy()  # ["contractcode", "farmername", "cruisedate"]
    if "path" in df.columns:
        cols_base.insert(0, "path")  # Añadir "path" al inicio si existe

    # --- Paso 2.5: Filtrar filas basura (sin tree_number y status_id) ---
    for col in ["tree_number", "status_id"]:
        if col not in df.columns:
            df[col] = pd.NA

    mask_empty = df["tree_number"].isna() & df["status_id"].isna()
    if mask_empty.sum() > 0:
        print(f"🧹 Filtradas {mask_empty.sum()} filas vacías sin tree_number ni status_id")
        df_blanks = df[mask_empty]
        df_blanks.to_csv("filas_basura_catalog.csv", index=False)
    df = df[~mask_empty]

    # --- Paso 3: Crear DataFrame del catálogo ---
    try:
        df_catalog = df[cols_base].drop_duplicates(subset=["contractcode", "cruisedate"]).copy()
    except KeyError as e:
        print(f"❌ Error crítico: {str(e)}")
        print("Columnas disponibles en df:", df.columns.tolist())
        raise

    # --- Paso 4: Calcular TreesSampled ---
    sampled = df.groupby(["contractcode", "cruisedate"]).size().reset_index(name="TreesSampled")

    # --- Paso 5: Traer datos de cat_farmers ---
    df_farmers = pd.read_sql("""
        SELECT contractcode, farmername, planting_year, treescontract AS trees_contract
        FROM masterdatabase.contract_farmer_information
    """, engine)


    df_farmers = pd.read_sql('SELECT "contractcode" AS contractcode, "farmername" FROM cat_farmers', engine)

    # --- Paso 6: Unir datos ---
    df_catalog = pd.merge(df_catalog, sampled, on=["contractcode", "cruisedate"], how="left")
    df_catalog = pd.merge(df_catalog, df_farmers, on="contractcode", how="left")

    # --- Paso 7: Ordenar columnas ---
    order = ["path", "contractcode", "farmername", "cruisedate", "planting_year", "trees_contract", "TreesSampled"]
    order = [col for col in order if col in df_catalog.columns]
    df_catalog = df_catalog[order]

    # --- Paso 7.5: Limpiar NaT para campos datetime ---
    for col in df_catalog.select_dtypes(include=["datetime64[ns]"]):
        df_catalog[col] = df_catalog[col].where(df_catalog[col].notna(), None)

    # --- Paso 8: Guardar en SQL ---
    ensure_table(
        df_catalog,
        engine,
        table_catalog_name,
        recreate=True
    )
    save_inventory_to_sql(df_catalog, engine, table_catalog_name, if_exists="replace")
    print(f"✅ Catálogo guardado: \n {table_catalog_name}")

    return df_catalog

def ensure_table(df, engine, table_name, recreate=False):
    insp = inspect(engine)

    with engine.begin() as conn:
        df = df.loc[:, ~df.columns.duplicated()]

        if recreate or not insp.has_table(table_name):
            if insp.has_table(table_name):
                conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))

            dtypes = get_dtypes_for_dataframe(df)
            df.head(0).to_sql(table_name, conn, index=False, if_exists="replace", dtype=dtypes)

            if 'id' in df.columns:
                conn.execute(text(
                    f'ALTER TABLE "{table_name}" '
                    f'ADD CONSTRAINT {table_name}_pk PRIMARY KEY (id)'
                ))
                print(f"🔑 PRIMARY KEY agregada en 'id'")
        else:
            existing_cols = {c['name'] for c in insp.get_columns(table_name)}
            for col in df.columns:
                if col not in existing_cols:
                    conn.execute(text(
                        f'ALTER TABLE "{table_name}" ADD COLUMN "{col}" TEXT'
                    ))

            # ✅ Verificar si 'id' ya tiene PK (para evitar conflicto ON CONFLICT)
            if 'id' in df.columns:
                result = conn.execute(text(f"""
                    SELECT COUNT(*)
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.constraint_column_usage ccu
                      ON tc.constraint_name = ccu.constraint_name
                    WHERE tc.table_name = :table AND tc.constraint_type = 'PRIMARY KEY'
                      AND ccu.column_name = 'id'
                """), {'table': table_name})
                has_pk = result.scalar() > 0

                if not has_pk:
                    try:
                        conn.execute(text(
                            f'ALTER TABLE "{table_name}" '
                            f'ADD CONSTRAINT {table_name}_pk PRIMARY KEY (id)'
                        ))
                        print(f"🔑 PRIMARY KEY añadida en tabla existente para 'id'")
                    except Exception as e:
                        print(f"❌ No se pudo agregar PK en 'id': {e}")


def save_inventory_to_sql(df,
        connection_string,
        table_name,
        if_exists="append",
        schema=None,
        dtype=None,
        progress=False,
        chunksize=1000):
    """Limpia nombres de columnas y guarda el DataFrame en SQL con tipos opcionales."""

    print("\n=== INICIO DE IMPORTACIÓN ===")
    #print("Columnas crudas del archivo:", df.columns.tolist())

    # AQUI: df ya viene renombrado y ordenado por prepare_df_for_sql,
    # así que NO lo tocamos más. Si quieres, mú evita duplicados:
    df = df.reindex(sorted(df.columns), axis=1)  # ordénalas alfabéticamente (o la clave que prefieras)

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

def cast_dataframe(df):
    """Convierte in-place las columnas presentes al dtype esperado."""
    from pandas import to_datetime
    for col, sa_type in get_dtypes_for_dataframe(df).items():        # ← ya existe
        pd_dtype = _SA_TO_PD.get(sa_type)
        if pd_dtype is None or col not in df.columns:
            continue
        if pd_dtype == "datetime64[ns]":
            df[col] = to_datetime(df[col], errors="coerce")
        else:
            df[col] = df[col].astype(pd_dtype, errors="ignore")
    return df

def marcar_lote_completado(batch_imports_path, tabla_destino, tabla_sql):
    from core.libs import Path, json

    ruta = Path(batch_imports_path)
    if not ruta.exists():
        print(f"❌ batch_imports.json no encontrado en {batch_imports_path}")
        return

    with open(ruta, encoding="utf-8") as f:
        lotes = json.load(f)

    for lote in lotes:
        if lote.get("tabla_destino") == tabla_destino:
            lote["estatus"] = "completado"
            lote["tabla_sql"] = tabla_sql
            print(f"✅ Lote marcado como completado en batch_imports.json")
            break
    else:
        print(f"❌ No se encontró tabla_destino={tabla_destino} en batch_imports.json")
        return

    with open(ruta, "w", encoding="utf-8") as f:
        json.dump(lotes, f, indent=2, ensure_ascii=False)


def upload_and_finalize(df_combined, df_good, df_bad, args, engine):
    if not df_good.empty:
        prepare_df_for_sql(df_good)
        df_good = cast_dataframe(df_good)

        save_inventory_to_sql(df_good, args.table, engine)

        if not df_bad.empty:
            df_bad.to_excel(f"{args.table}_errores.xlsx", index=False)

        # Exportar duplicados
        duplicated = df_good[df_good.duplicated(["contractcode", "plot", "tree"], keep=False)]
        if not duplicated.empty:
            duplicated.to_excel(f"{args.table}_duplicados.xlsx", index=False)

        # Guardar Excel combinado
        df_combined.to_excel(f"{args.table}_completo.xlsx", index=False)

        # Auditoría
        run_audit(args.table, engine)
        create_inventory_catalog(args.table, engine)

        # Marcar como completado
        marcar_lote_completado(args.table)
