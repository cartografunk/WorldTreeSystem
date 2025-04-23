# utils/column_mapper.py
from utils.schema import COLUMNS

# Para get_column: de etiqueta lógica → listado de aliases
COLUMN_LOOKUP = {
    logical: entry["internal"]
    for entry in COLUMNS
    for logical in entry["aliases"] + [entry["internal"], entry["sql_name"]]
}

# Para renombrar internamente → SQL
SQL_COLUMNS = {
    entry["internal"]: entry["sql_name"]
    for entry in COLUMNS
}