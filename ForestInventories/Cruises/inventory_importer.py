# forest_inventory/inventory_importer.py
from utils.libs import pd, unicodedata, re  # 👈 Añadir 're' aquí
from utils.db import get_engine


def save_inventory_to_sql(df, connection_string, table_name, if_exists="append", schema=None, dtype=None):
    """Limpia nombres de columnas y guarda el DataFrame en SQL con tipos opcionales."""

    print("\n=== INICIO DE IMPORTACIÓN ===")
    print("Columnas crudas del archivo:", df.columns.tolist())


    def clean_column_name(name):
        """Versión mejorada para manejar múltiples casos especiales"""
        name = str(name)
        # Paso 1: Eliminar símbolos (#) y espacios ANTES de normalizar
        name = re.sub(r'[#\s]+', '_', name)  # 👈 ¡Cambio clave!
        # Paso 2: Normalizar caracteres unicode (ej: Á → A)
        name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('ascii')
        # Paso 3: Eliminar caracteres no alfanuméricos (excepto _)
        name = re.sub(r'[^\w_]', '', name)
        # Paso 4: Limpieza final
        name = name.strip('_').lower()
        name = re.sub(r'_+', '_', name)
        return name

    # Aplicar limpieza y verificar
    df.columns = [clean_column_name(col) for col in df.columns]
    print("Columnas normalizadas:", df.columns.tolist())  # 👈 Debug crucial

    # Eliminar columnas duplicadas y vacías
    df = df.loc[:, ~df.columns.duplicated()]
    df.dropna(how='all', inplace=True)

    print("Nombre normalizado de '# Árbol':", clean_column_name("# Árbol"))  # Debe imprimir "arbol"


    try:
        engine = get_engine()
        # Bulk insert parametrizado
        conn = engine.raw_connection()
        cursor = conn.cursor()

        cols = df.columns.tolist()
        cols_quoted = ", ".join([f'"{c}"' for c in cols])
        placeholders = ", ".join(["%s"] * len(cols))

        table_full = f'{schema + "." if schema else ""}"{table_name}"'
        insert_query = (
                    f"INSERT INTO {table_full} ({cols_quoted}) VALUES ({placeholders})"
        )

        data = df.values.tolist()
        cursor.executemany(insert_query, data)
        conn.commit()
        cursor.close()
        conn.close()

        print(f"✅ Bulk insert completado: '{table_name}' ({len(data)} filas)")
    except Exception as e:
        print(f"❌ Error al realizar bulk insert: {str(e)}")

        raise
