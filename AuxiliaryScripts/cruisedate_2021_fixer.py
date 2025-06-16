from core.db import get_engine
from sqlalchemy import inspect, text

def parchar_cruisedate_2021(schema="public"):
    engine = get_engine()
    insp = inspect(engine)
    tablas = insp.get_table_names(schema=schema)
    tablas_2021 = [
        t for t in tablas
        if (t.startswith("inventory_") or t.startswith("cat_inventory_")) and t.endswith("2021")
    ]

    with engine.connect() as conn:
        for tabla in tablas_2021:
            print(f"🩹 Parchando {tabla} ...")
            update_sql = f"""
                UPDATE {schema}.{tabla}
                SET cruisedate = '2021-01-01'
                WHERE cruisedate IS NULL OR cruisedate = '' OR cruisedate = 'NaT';
            """
            result = conn.execute(text(update_sql))
            print(f"✅ {tabla} parchada.")

if __name__ == "__main__":
    parchar_cruisedate_2021()
