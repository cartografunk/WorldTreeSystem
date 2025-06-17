#InventoryMetrics/generate_helpers
import re

import pandas


def safe_numeric(series):
    try:
        return pd.to_numeric(series, errors="coerce")
    except Exception:
        return pd.Series([None] * len(series), index=series.index)


def create_cat_inventory_tables(engine, tables: list[str]):
    """
    Genera las tablas cat_inventory_<country>_<year> a partir de cada tabla inventory_<country>_<year>.
    """
    for table in tables:
        m = re.match(r"inventory_([a-z]+)_(\d{4})", table)
        if not m:
            continue
        country, year = m.groups()
        target = f"cat_inventory_{country}_{year}"

        try:
            df = pd.read_sql(f"""
                SELECT DISTINCT contractcode, cruisedate
                FROM public.{table}
                WHERE contractcode IS NOT NULL
            """, engine)

            df.to_sql(target, engine, if_exists="replace", index=False)
            print(f"📁 Generada: {target} ({len(df)} contratos)")

        except Exception as e:
            print(f"⚠️  No se pudo crear {target}: {e}")
