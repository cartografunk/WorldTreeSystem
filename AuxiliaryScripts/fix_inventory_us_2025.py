from core.db import get_engine
from core.schema_helpers import rename_columns_using_schema, FINAL_ORDER, get_dtypes_for_dataframe
from core.libs import pd
from sqlalchemy import text
from CruisesProcessor.catalog_normalizer import normalize_catalogs

def fix_inventory_us():
    engine = get_engine()
    table = "inventory_us_2025_backup"
    backup_table = table + "2"

    # 1. Carga el DataFrame desde SQL
    print(f"🔎 Leyendo tabla {table}...")
    df = pd.read_sql_table(table, engine)
    print("🧐 Columnas originales:", df.columns.tolist())
    print(df.head())

    # 2. Renombra columnas usando el schema SOLO para las que existen
    df = rename_columns_using_schema(df)

    # Normaliza todos los catálogos importantes
    df = normalize_catalogs(df, engine)
    # Drop columnas textuales crudas de catálogos
    raw_fields = ["Status", "Species", "Defect", "Disease", "Pests", "Coppiced", "Permanent Plot"]
    drop_cols = []

    df.drop(raw_fields, axis=1, inplace=True)

    # 3. Para cada columna de FINAL_ORDER:
    #    - Si existe: intenta castear al tipo correcto
    #    - Si no existe: crea columna nueva vacía (pd.NA)
    dtypes = get_dtypes_for_dataframe(df)
    for col in FINAL_ORDER:
        if col in df.columns:
            try:
                dtype = dtypes.get(col)
                if dtype is not None:
                    if "datetime" in str(dtype):
                        df[col] = pd.to_datetime(df[col], errors="coerce")
                    elif "float" in str(dtype) or "numeric" in str(dtype) or "double" in str(dtype):
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                    elif "int" in str(dtype):
                        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                    else:
                        df[col] = df[col].astype(str)
            except Exception as e:
                print(f"⚠️ Error casteando {col}: {e}")
        else:
            print(f"⚠️ Columna ausente, creando vacía: {col}")
            df[col] = pd.NA

    # 4. Arreglo NA/None en todo el DataFrame para evitar errores SQL
    df = df.replace({pd.NA: None, "<NA>": None, "NaT": None, "nan": None})
    df = df.where(pd.notnull(df), None)

    # 5. Reordena columnas (FINAL_ORDER primero, luego las extras)
    cols_in_final = [col for col in FINAL_ORDER if col in df.columns]
    extra_cols = [col for col in df.columns if col not in FINAL_ORDER]
    df = df[cols_in_final + extra_cols]

    # 6. Respaldar tabla vieja (si no existe)
    with engine.begin() as conn:
        print(f"💾 Respaldando {table} como {backup_table}...")
        conn.execute(text(f'CREATE TABLE IF NOT EXISTS "{backup_table}" AS TABLE "{table}";'))

    # 7. Guarda la tabla arreglada (reemplazando)
    print(f"💾 Escribiendo tabla arreglada como {table} (reemplazando)...")
    df.to_sql(table, engine, if_exists="replace", index=False, dtype=dtypes)

    print("✅ Fix terminado. Tabla reordenada y normalizada según schema.")

    # 8. (Opcional) Agrega PRIMARY KEY en id si aplica
    try:
        with engine.begin() as conn:
            conn.execute(text(f'ALTER TABLE "{table}" ADD PRIMARY KEY (id);'))
        print("🔑 PRIMARY KEY agregada en 'id'.")
    except Exception as e:
        print(f"⚠️ No se pudo agregar PRIMARY KEY (id): {e}")

if __name__ == "__main__":
    fix_inventory_us()
