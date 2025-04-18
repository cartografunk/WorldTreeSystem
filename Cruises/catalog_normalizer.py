# catalog_normalizer.py

from utils.libs import pd
from utils.column_mapper import COLUMN_LOOKUP
from sqlalchemy import text

PAIS_CONFIG = {
    "GT": {"col": "nombre"},
    "MX": {"col": "nombre"},
    "CR": {"col": "nombre"},
    "US": {"col": "nombre_en"},
}

ALIASES = {
    "yes": "sí",
    "si": "sí",
    "sí": "sí",
    "no": "no"
}


def parse_catalog_value(val: str):
    """
    Extrae el valor textual limpiándolo de prefijos numéricos.
    Por ejemplo, "3) Elongata" retorna "Elongata".
    """
    if pd.isna(val):
        return None
    val_str = str(val).strip()
    if ')' in val_str:
        return val_str.split(')', 1)[1].strip()
    return val_str


def find_existing_column(df, logical_name):
    """
    Retorna la primera columna del DataFrame que coincida con alguno de los alias del campo lógico.
    """
    posibles = COLUMN_LOOKUP.get(logical_name, [])
    for alias in posibles:
        if alias in df.columns:
            return alias
    return None


def normalize_catalogs(df: pd.DataFrame, engine, catalog_columns: dict, country_code='GT') -> pd.DataFrame:
    """
    Normaliza los campos de catálogo del DataFrame reemplazando los valores de texto por los IDs correspondientes.
    La búsqueda se hace usando el campo configurado según el país (para GT, MX, CR se usa "nombre"; para US, "nombre_en").
    Si el valor no existe en el catálogo, se realiza un SELECT para verificarlo; si no se encuentra, se inserta y se obtiene el nuevo id.

    Args:
        df (pd.DataFrame): DataFrame con datos de inventario.
        engine: SQLAlchemy engine.
        catalog_columns (dict): Diccionario {campo_lógico: tabla_sql}, por ejemplo, {'Species': 'cat_species'}
        country_code (str): Código de país para determinar el campo de referencia (por defecto "GT").

    Returns:
        pd.DataFrame modificado con una nueva columna *_id para cada catálogo normalizado.
    """
    config = PAIS_CONFIG.get(country_code.upper(), {"col": "nombre"})
    field = config["col"]
    df_result = df.copy()

    with engine.begin() as conn:
        for logical_col, cat_table in catalog_columns.items():
            actual_col = find_existing_column(df_result, logical_col)
            if not actual_col:
                print(f"⚠️ Columna '{logical_col}' no encontrada en el DataFrame.")
                continue

            print(f"\n🔁 Normalizando: {logical_col} → {cat_table} ({field})")
            unique_vals = df_result[actual_col].dropna().unique()
            val_map = {}

            # Obtener el catálogo actual (solicitando solo el campo de referencia)
            existing = conn.execute(
                text(f"SELECT id, {field} FROM {cat_table}")
            ).mappings().all()

            catalog_dict = {
                str(row[field]).strip().lower(): row["id"]
                for row in existing if row[field]
            }

            for val in unique_vals:
                raw_val = str(val).strip()
                parsed_val = parse_catalog_value(raw_val)
                lookup_val = ALIASES.get(parsed_val.lower(), parsed_val.lower())

                if lookup_val in catalog_dict:
                    val_map[raw_val] = catalog_dict[lookup_val]
                    continue

                # Primero, consulta para verificar si el valor existe
                select_query = text(f"SELECT id FROM {cat_table} WHERE {field} = :val")
                result = conn.execute(select_query, {"val": parsed_val})
                existing_id = result.scalar()
                if existing_id:
                    val_map[raw_val] = existing_id
                    catalog_dict[lookup_val] = existing_id
                    continue

                # Si no existe, se inserta
                insert_query = text(f"""
                    INSERT INTO {cat_table} ({field})
                    VALUES (:val)
                    RETURNING id
                """)
                result = conn.execute(insert_query, {"val": parsed_val})
                new_id = result.scalar()

                val_map[raw_val] = new_id
                catalog_dict[lookup_val] = new_id
                print(f"🆕 Insertado '{parsed_val}' en {cat_table} → id {new_id}")

            id_col = f"{logical_col.lower().replace(' ', '_')}_id"
            df_result[id_col] = df_result[actual_col].map(val_map)
            print(f"✅ Columna '{id_col}' asignada en el DataFrame")

    return df_result
