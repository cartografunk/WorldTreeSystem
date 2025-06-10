#Cruises/catalog_normalizer.py
from core.libs import pd, re, unicodedata
from core.schema import COLUMNS
from core.schema_helpers import get_column
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

import re

def parse_country_code(tabla_destino: str) -> str:
    # Busca el patrón inventory_{cc}_{year} (por ejemplo: inventory_cr_2025)
    m = re.match(r"inventory_([a-z]{2})_\d{4}", tabla_destino, re.IGNORECASE)
    if m:
        return m.group(1).upper()
    raise ValueError(f"No se pudo extraer country_code de '{tabla_destino}'")


def parse_catalog_value(val: str):
    if pd.isna(val):
        return None
    val_str = str(val).strip()
    if ')' in val_str:
        return val_str.split(')', 1)[1].strip()
    return val_str



def normalize_catalogs(df, engine, country_code):
    for col_def in COLUMNS:
        if col_def["source"] != "input" or "catalog_table" not in col_def:
            continue

        raw_col = col_def["key"]         # p. ej. "Pests" o "Defect" (tal como quedó tras rename_columns)
        catalog = col_def["catalog_table"]  # p. ej. "cat_pest"
        campo_texto = PAIS_CONFIG[country_code.upper()]["col"]
        campo_id = "id"

        # 1) Leer todo el catálogo desde SQL:
        qry = text(f'SELECT {campo_id}, {campo_texto} FROM public."{catalog}"')
        cat_df = pd.read_sql(qry, engine)

        # 2) Armar un mapping texto→id (asegura incluir ambas columnas si las hay)
        #    Normalizamos con .str.strip() para quitar espacios en blanco:
        cat_df[campo_texto] = cat_df[campo_texto].astype(str).str.strip()
        val_map = dict(zip(cat_df[campo_texto], cat_df[campo_id]))

        # 3) Aplicar el mapeo a la columna raw; si el texto no existe, dejar NaN
        df[col_def["key"] + "_raw"] = df[raw_col].astype(str).str.strip()
        df[col_def["key"] + "_id"] = df[col_def["key"] + "_raw"].map(val_map).astype("Int64")

        # 4) (Opcional) Si quieres eliminar la columna raw después de convertir:
        df.drop(columns=[raw_col, col_def["key"] + "_raw"], inplace=True)

    return df
