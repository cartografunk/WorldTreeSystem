# inventory_metrics/generate.py

from tqdm import tqdm
from InventoryMetrics.processing_metrics import aggregate_contracts
from InventoryMetrics.generate_helpers import safe_numeric
from InventoryMetrics.inventory_retriever import get_inventory_tables, get_cruise_date
from InventoryMetrics.generate_helpers import clean_and_fuse_metrics

from core.libs import re, pd, text
from core.db import get_engine
from core.schema_helpers import get_column

# def upsert_metrics(engine, rows):
#     if not rows:
#         return
#
#     df = pd.DataFrame(rows)
#
#     # Asegurar que la tabla tenga la estructura correcta
#     with engine.begin() as conn:
#         conn.execute(text("""
#             CREATE TABLE IF NOT EXISTS masterdatabase.inventory_metrics (
#                 contract_code TEXT,
#                 inventory_year INTEGER,
#                 inventory_date TEXT,
#                 dbh_mean NUMERIC,
#                 dbh_std NUMERIC,
#                 tht_mean NUMERIC,
#                 tht_std NUMERIC,
#                 mht_mean NUMERIC,
#                 mht_std NUMERIC,
#                 doyle_bf_mean NUMERIC,
#                 doyle_bf_std NUMERIC,
#                 doyle_bf_total NUMERIC,
#                 pkid TEXT PRIMARY KEY
#             )
#         """))
#
#         for _, row in df.iterrows():
#             conn.execute(text("""
#                 INSERT INTO masterdatabase.inventory_metrics (
#                     contract_code, inventory_year, inventory_date,
#                     dbh_mean, dbh_std,
#                     tht_mean, tht_std,
#                     mht_mean, mht_std,
#                     doyle_bf_mean, doyle_bf_std, doyle_bf_total
#                 ) VALUES (
#                     :pkid, :contract_code, :inventory_year, :inventory_date,
#                     :dbh_mean, :dbh_std,
#                     :tht_mean, :tht_std,
#                     :mht_mean, :mht_std,
#                     :doyle_bf_mean, :doyle_bf_std, :doyle_bf_total
#                 )
#                 ON CONFLICT (pkid) DO UPDATE SET
#                     contract_code = EXCLUDED.contract_code,
#                     inventory_year = EXCLUDED.inventory_year,
#                     inventory_date = EXCLUDED.inventory_date,
#                     dbh_mean = EXCLUDED.dbh_mean,
#                     dbh_std = EXCLUDED.dbh_std,
#                     tht_mean = EXCLUDED.tht_mean,
#                     tht_std = EXCLUDED.tht_std,
#                     mht_mean = EXCLUDED.mht_mean,
#                     mht_std = EXCLUDED.mht_std,
#                     doyle_bf_mean = EXCLUDED.doyle_bf_mean,
#                     doyle_bf_std = EXCLUDED.doyle_bf_std,
#                     doyle_bf_total = EXCLUDED.doyle_bf_total
#             """), row.to_dict())


def main():
    print("📊 Generando métricas de inventario...")
    engine = get_engine()
    tables = get_inventory_tables(engine)

    all_dfs = []
    for table in tqdm(tables, desc="Procesando tablas"):
        # Carga la tabla de inventario
        df = pd.read_sql(f"SELECT * FROM public.{table}", engine)
        # Obtén país/año desde el nombre de la tabla
        m = re.match(r"inventory_([a-z]+)_(\d{4})", table)
        country, year = m.groups()
        year = int(year)
        # Agrupa y calcula métricas
        df_metrics = aggregate_contracts(
            df,
            engine,
            country=country,
            year=year,
            include_all_contracts=True  # <-- Así aseguras que no falte ninguno
        )
        all_dfs.append(df_metrics)

    df_full = pd.concat(all_dfs, ignore_index=True)
    df_final = clean_and_fuse_metrics(df_full)

    df_final.to_sql("inventory_metrics", engine, schema="masterdatabase", if_exists="replace", index=False)
    print("✅ Métricas insertadas en masterdatabase.inventory_metrics")

if __name__ == "__main__":
    main()
