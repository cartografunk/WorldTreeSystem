# InventoryMetrics/planting_times.py

from core.db import get_engine
from core.libs import pd, to_datetime, text

def get_planting_info(engine, fields=("contract_code", "planting_year", "planting_date")):
    fields_sql = ", ".join(fields)
    sql = f"SELECT {fields_sql} FROM maindatabase.contract_tree_information"
    return pd.read_sql(sql, engine)


def months_diff(date1, date2):
    if pd.isnull(date1) or pd.isnull(date2):
        return None
    d1, d2 = pd.to_datetime(date1), pd.to_datetime(date2)
    delta = (d2.year - d1.year) * 12 + (d2.month - d1.month)
    delta += (d2.day - d1.day) / 30.44
    return round(delta, 1)

def pretty_tree_age(date1, date2):
    """
    Calcula la edad entre dos fechas y la devuelve como 'año mes/12', ej: 1 6/12
    """
    import pandas as pd
    if pd.isnull(date1) or pd.isnull(date2):
        return None
    d1, d2 = pd.to_datetime(date1), pd.to_datetime(date2)
    total_months = (d2.year - d1.year) * 12 + (d2.month - d1.month)
    day_frac = (d2.day - d1.day) / 30.44
    months_decimal = total_months + day_frac
    years = int(months_decimal // 12)
    months = int(round(months_decimal % 12))
    if months == 12:
        years += 1
        months = 0
    return f"{years} {months}/12"


def update_tree_age_metrics(where="1=1"):
    engine = get_engine()
    # 1. Traer planting_date por contrato con filtro dinámico
    contracts = pd.read_sql(
        f"SELECT contract_code, planting_date FROM masterdatabase.contract_tree_information WHERE {where}", engine)
    contracts["planting_date"] = pd.to_datetime(contracts["planting_date"])

    # 2. Traer metrics con fechas SOLO para los contratos seleccionados
    # (opcional: podrías filtrar aquí también, pero basta con el merge)
    metrics = pd.read_sql(
        "SELECT contract_code, inventory_date FROM masterdatabase.inventory_metrics", engine)
    metrics["inventory_date"] = pd.to_datetime(metrics["inventory_date"])

    # 3. Merge y cálculo SOLO para los contratos filtrados
    merged = metrics.merge(contracts, how="inner", on="contract_code")
    merged["tree_age"] = [
        pretty_tree_age(row["planting_date"], row["inventory_date"])
        for _, row in merged.iterrows()
    ]

    # 4. Actualiza tree_age solo para ese subconjunto
    with engine.begin() as conn:
        for _, row in merged.iterrows():
            conn.execute(
                text(
                    "UPDATE masterdatabase.inventory_metrics "
                    "SET tree_age = :tree_age "
                    "WHERE contract_code = :contract_code AND inventory_date = :inventory_date"
                ),
                {
                    "tree_age": row["tree_age"],
                    "contract_code": row["contract_code"],
                    "inventory_date": row["inventory_date"].date() if not pd.isnull(row["inventory_date"]) else None
                }
            )
    print(f"✅ tree_age actualizado para contratos filtrados con: {where}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--where', type=str, default="1=1", help="Filtro WHERE SQL, ej: \"region = 'Guatemala'\" o \"contract_code LIKE 'GT%'\"")
    args = parser.parse_args()
    update_tree_age_metrics(where=args.where)