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