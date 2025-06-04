#Cruises/catalog_normalizer.py
from core.libs import pd, re, unicodedata
from core.schema import COLUMNS
from core.schema import get_column
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
    if pd.isna(val):
        return None
    val_str = str(val).strip()
    if ')' in val_str:
        return val_str.split(')', 1)[1].strip()
    return val_str


def normalize_catalogs(df: pd.DataFrame, engine, logical_keys: list[str], country_code= str) -> pd.DataFrame:
    config = PAIS_CONFIG.get(country_code.upper(), {"col": "nombre"})
    field = config["col"]
    df_result = df.copy()
    normalizadas = []

    print("=== Normalización de catálogos ===")

    with engine.begin() as conn:
        for logical in logical_keys:
            col_entry = next(
                (c for c in COLUMNS if c["key"] == logical and "catalog_table" in c),
                None
            )
            if not col_entry:
                print(f"⚠️ Lógica '{logical}' no está registrada como campo de catálogo en schema.")
                continue

            table = col_entry["catalog_table"]
            id_field = col_entry.get("catalog_field", "id")
            dest_col = col_entry["sql_name"]

            try:
                raw_col = get_column(logical, df_result)
            except KeyError:
                print(f"⚠️ Columna '{logical}' no encontrada en el DataFrame.")
                continue

            unique_vals = df_result[raw_col].dropna().unique()
            val_map = {}

            # Cargar catálogo actual
            existing = conn.execute(
                text(f"SELECT {id_field}, {field} FROM {table}")
            ).mappings().all()

            catalog_dict = {
                str(row[field]).strip().lower(): row[id_field]
                for row in existing if row[field]
            }

            for val in unique_vals:
                raw_val = str(val).strip()
                parsed_val = parse_catalog_value(raw_val)
                lookup_val = ALIASES.get(parsed_val.lower(), parsed_val.lower())

                if lookup_val in catalog_dict:
                    val_map[raw_val] = catalog_dict[lookup_val]
                    continue

                # Intentar encontrar directamente
                result = conn.execute(
                    text(f"SELECT {id_field} FROM {table} WHERE {field} = :val"),
                    {"val": parsed_val}
                )
                existing_id = result.scalar()
                if existing_id:
                    val_map[raw_val] = existing_id
                    catalog_dict[lookup_val] = existing_id
                    continue

                # Insertar si no existe
                try:
                    result = conn.execute(
                        text(f"INSERT INTO {table} ({field}) VALUES (:val) RETURNING {id_field}"),
                        {"val": parsed_val}
                    )
                    new_id = result.scalar()
                    val_map[raw_val] = new_id
                    catalog_dict[lookup_val] = new_id
                    print(f"🆕 Insertado '{parsed_val}' en {table} → {id_field}={new_id}")
                except Exception as e:
                    print(f"❌ Error al insertar '{parsed_val}' en {table}: {e}")
                    continue

            df_result[dest_col] = df_result[raw_col].map(val_map)
            normalizadas.append(dest_col)

    print(f"✅ Columnas normalizadas: {', '.join(normalizadas)}")
    return df_result
