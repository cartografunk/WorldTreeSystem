# InventoryMetrics/planting_times.py

from core.libs import pd

def get_planting_info(engine, fields=("contract_code", "planting_year", "planting_date")):
    fields_sql = ", ".join(fields)
    sql = f"SELECT {fields_sql} FROM maindatabase.contract_tree_information"
    return pd.read_sql(sql, engine)

