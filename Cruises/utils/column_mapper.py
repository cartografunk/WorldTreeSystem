# utils/column_mapper.py
from core.schema import COLUMNS

COLUMN_LOOKUP = {
    alias: col["key"]
    for col in COLUMNS
    for alias in col["aliases"] + [col["key"], col["sql_name"]]
}

SQL_COLUMNS = {
    col["key"]: col["sql_name"]
    for col in COLUMNS
}