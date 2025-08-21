#core/schema_helpers_db_management.py
from core.schema_helpers import get_column, clean_column_name  # si estás dentro del mismo archivo, omite este import
from typing import Dict, List, Callable, Optional

# --- SOLO reemplaza el bloque TABLE_KEYS por este ---

TABLE_KEYS: Dict[str, List[str]] = {
    # masterdatabase.contract_farmer_information
    "cfi": [
        "contractname", "representative", "farmernumber", "phone", "email",
        "address", "shippingaddress", "region", "status", "notes"
    ],
    # masterdatabase.contract_tree_information
    # ⛔️ Quitamos 'harvest_year_10' (se calcula), 'latitude', 'longitude'
    "cti": [
        "plantingyear", "treescontract", "planted", "strain",
        "plantingdate", "species", "land_location_gps"
    ],
    # masterdatabase.contract_allocation (si aplica)
    "ca": [
        "usa_trees_planted", "total_can_allocation"
    ],
}


# Cómo se llaman las columnas en BD (snake_case). Centralizar **aquí** excepciones.
DB_COLUMNS: Dict[str, str] = {
    # CFI
    "contractname": "contract_name",
    "farmernumber": "farmer_number",
    "shippingaddress": "shipping_address",
    # CTI
    "plantingyear": "planting_year",
    "treescontract": "trees_contract",
    "plantingdate": "planting_date",
    # el resto ya están en snake o coinciden con key
    # CA (si aplica)
    # "usa_trees_planted": "usa_trees_planted",
    # "total_can_allocation": "total_can_allocation",
}

def key_to_db_col(key: str) -> str:
    """Convierte una key lógica a nombre de columna en BD."""
    return DB_COLUMNS.get(key, clean_column_name(key))

def read_cell_by_key(row, headers: List[str], hdr_df, logical_key: str):
    """
    Usa schema (aliases incluidos) para hallar el nombre real en el sheet y leer su valor.
    - hdr_df: DataFrame 'fantasma' con solo headers (como usas ya).
    """
    try:
        real = get_column(logical_key, hdr_df)
    except KeyError:
        return None
    try:
        idx = headers.index(real)  # 0-based
    except ValueError:
        return None
    return row[idx].value

def extract_group_params(
    row,
    headers: List[str],
    hdr_df,
    group: str,
    transforms: Optional[Dict[str, Callable]] = None
) -> Dict[str, object]:
    """
    Extrae valores desde la fila del Excel según las keys del grupo (cfi/cti/ca),
    aplicando transforms por key si se proveen, y devolviendo dict {db_col: value}.
    """
    params = {}
    for key in TABLE_KEYS.get(group, []):
        raw = read_cell_by_key(row, headers, hdr_df, key)
        if transforms and key in transforms:
            raw = transforms[key](raw)
        params[key_to_db_col(key)] = raw
    return params
