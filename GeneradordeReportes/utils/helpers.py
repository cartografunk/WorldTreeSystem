import os
import matplotlib.pyplot as plt

def guardar_figura(path, fig, facecolor='white'):
    if not os.path.exists(path):
        fig.savefig(path, dpi=300, bbox_inches='tight', facecolor=facecolor)
        print(f"✅ Guardado: {path}")
    else:
        print(f"⚠️ Ya existe y no se sobreescribió: {path}")
    plt.close(fig)


def get_region_language(country_code: str = "CR") -> str:
    """
    Retorna 'es' para CR, GT, MX; 'en' para US; por defecto 'es'.
    """
    mapping = {
        "CR": "es",
        "GT": "es",
        "MX": "es",
        "US": "en",
    }
    return mapping.get(country_code.upper(), "es")

def get_inventory_table_name(country: str, year: int) -> str:
    return f"inventory_{country.lower()}_{year}"

from Cruises.utils.schema import COLUMNS

def get_sql_column(key: str) -> str:
    match = next((col for col in COLUMNS if col["key"] == key), None)
    if match:
        return match["sql_name"]
    raise KeyError(f"Key '{key}' not found in schema.")


from sqlalchemy import inspect
from Cruises.utils.schema import COLUMNS as schema

def resolve_column(engine, table_name: str, key: str) -> str:
    """
    A partir de un key (p.ej. 'contractcode') busca en schema['sql_name'] y sus aliases
    y devuelve el nombre de columna EXACTO que existe en la tabla.
    """
    # Busca la definición de ese key
    entry = next((e for e in schema if e['key'] == key), None)
    if not entry:
        raise KeyError(f"No existe definición de esquema para key '{key}'")

    # Lista de candidatos en orden: sql_name + aliases
    candidates = [entry['sql_name']] + entry.get('aliases', [])
    # Inspector para ver las columnas reales
    tbl = table_name.split('.')[-1]
    inspector = inspect(engine)
    real_cols = [c['name'] for c in inspector.get_columns(tbl)]

    for cand in candidates:
        if cand in real_cols:
            return cand

    raise KeyError(f"Ninguno de {candidates} existe en {table_name}. Columnas reales: {real_cols}")