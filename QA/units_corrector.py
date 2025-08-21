# InventoryQA/unit_corrector.py
from core.libs import pd
from core.db import get_engine
from core.schema_helpers import get_column
from core.units import UNITS_BLOCK
from core.doyle_calculator import calculate_doyle
from core.schema import COLUMNS

# 1. Lista de tablas a corregir
TABLES_TO_FIX = [
    "inventory_cr_2021",
    "inventory_cr_2024",
    "inventory_cr_2025",
    "inventory_gt_2024",
    "inventory_gt_2025",
    "inventory_mx_2021",
    "inventory_mx_2025"
]

def get_convertible_fields():
    """
    Devuelve lista de tuplas: (nombre_columna, unit_type)
    Solo las marcadas como convertible en el schema.
    """
    return [
        (col["key"], col.get("unit_type"))
        for col in COLUMNS
        if col.get("convertible") and col.get("unit_type")
    ]

def convert_column(series, field_key, from_unit, to_unit):
    factor = UNITS_BLOCK[field_key][to_unit]["factor"] / UNITS_BLOCK[field_key][from_unit]["factor"]
    return pd.to_numeric(series, errors="coerce") * factor

def correct_table(engine, table):
    print(f"Corrigiendo {table}...")
    df = pd.read_sql(f"SELECT * FROM public.{table}", engine)
    for col, unit_type in get_convertible_fields():
        real_col = get_column(col, df)
        factor = UNITS_BLOCK[unit_type]["en"]["factor"] / UNITS_BLOCK[unit_type]["es"]["factor"]
        print(f" - Corrigiendo {real_col} ({unit_type}): es → en (x{factor})")
        df[real_col] = pd.to_numeric(df[real_col], errors="coerce") * factor
    df = calculate_doyle(df, force_recalc=True)
    df["units_corrected"] = True
    df.to_csv(f"{table}_backup_before_units.csv", index=False)
    df.to_sql(table, engine, if_exists="replace", index=False)
    print(f"✅ {table} corregida y respaldada.")

def main():
    engine = get_engine()
    for table in TABLES_TO_FIX:
        correct_table(engine, table)
    print("\nTodos los inventarios corregidos. Vuelve a correr InventoryMetrics para refrescar métricas.")

if __name__ == "__main__":
    main()
