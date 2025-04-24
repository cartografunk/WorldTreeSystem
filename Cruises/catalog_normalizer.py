# NUEVA VERSIÓN – catalog_normalizer.py
from utils.libs import pd, tqdm
from utils.schema import COLUMNS
from utils.cleaners import get_column, clean_column_name
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

def find_catalog_entry(logical_key: str):
    logical_key_cf = logical_key.strip().casefold()
    for col in COLUMNS:
        if "catalog" not in col:
            continue
        keys_to_match = [col["key"], col["sql_name"]] + col.get("aliases", [])
        if any(logical_key_cf == k.strip().casefold() for k in keys_to_match):
            return col
    return None



def normalize_catalogs(df: pd.DataFrame, engine, logical_keys: list[str], country_code='GT') -> pd.DataFrame:
    config = PAIS_CONFIG.get(country_code.upper(), {"col": "nombre"})
    field = config["col"]
    df_result = df.copy()

    results = []

    with engine.begin() as conn:
        for logical in tqdm(logical_keys, desc="🔁 Normalizando catálogos", ncols=90):
            col_entry = find_catalog_entry(logical)
            if not col_entry:
                results.append((logical, "⚠️ No registrado en schema"))
                continue

            table = col_entry["catalog"]
            dest_col = col_entry["sql_name"]

            try:
                raw_col = get_column(df_result, logical)
            except KeyError:
                results.append((logical, "❌ No encontrada en DataFrame"))
                continue

            unique_vals = df_result[raw_col].dropna().unique()
            val_map = {}

            existing = conn.execute(
                text(f"SELECT id, {field} FROM {table}")
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

                result = conn.execute(
                    text(f"SELECT id FROM {table} WHERE {field} = :val"),
                    {"val": parsed_val}
                )
                existing_id = result.scalar()
                if existing_id:
                    val_map[raw_val] = existing_id
                    catalog_dict[lookup_val] = existing_id
                    continue

                result = conn.execute(
                    text(f"""INSERT INTO {table} ({field}) VALUES (:val) RETURNING id"""),
                    {"val": parsed_val}
                )
                new_id = result.scalar()
                val_map[raw_val] = new_id
                catalog_dict[lookup_val] = new_id

            df_result[dest_col] = df_result[raw_col].map(val_map)
            results.append((logical, f"✅ '{dest_col}' asignada"))

    print("\n📋 Resumen de normalización de catálogos:")
    for logical, result in results:
        print(f" • {logical:<15} → {result}")

    return df_result

