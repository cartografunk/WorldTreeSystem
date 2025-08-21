# inventory_metrics/generate.py

from tqdm import tqdm
from InventoryMetrics.processing_metrics import aggregate_contracts
from InventoryMetrics.generate_helpers import safe_numeric
from InventoryMetrics.inventory_retriever import get_inventory_tables, get_cruise_date
from InventoryMetrics.generate_helpers import clean_and_fuse_metrics

from core.libs import re, pd, text, argparse
from core.db import get_engine
from core.schema_helpers import get_column
from core.db import backup_table

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

from InventoryMetrics.generate_helpers import create_cat_inventory_tables
from InventoryMetrics.inventory_retriever import get_inventory_tables

from InventoryMetrics.planting_times import pretty_tree_age
# (Asegúrate de tener esta función disponible aquí, o cópiala si hace falta)

def main(where="1=1"):
    print("📊 Generando métricas de inventario...")
    engine = get_engine()
    tables = get_inventory_tables(engine)
    create_cat_inventory_tables(engine, tables)

    all_dfs = []
    for table in tqdm(tables, desc="Procesando tablas"):
        df = pd.read_sql(f"SELECT * FROM public.{table} WHERE {where}", engine)
        m = re.match(r"inventory_([a-z]+)_(\d{4})", table)
        country, year = m.groups()
        year = int(year)
        df_metrics = aggregate_contracts(
            df,
            engine,
            country=country,
            year=year,
            include_all_contracts=True
        )
        all_dfs.append(df_metrics)

    df_full = pd.concat(all_dfs, ignore_index=True)
    df_final = clean_and_fuse_metrics(df_full)

    # --- Añade el campo type_of_metric si no existe ---
    if "type_of_metric" not in df_final.columns:
        df_final["type_of_metric"] = "inventory"
    else:
        df_final["type_of_metric"] = df_final["type_of_metric"].fillna("inventory")

    # --- AQUÍ HACES EL MERGE Y CÁLCULO ---
    # Trae planting_year y planting_date
    contracts = pd.read_sql(
        "SELECT contract_code, planting_year, planting_date FROM masterdatabase.contract_tree_information",
        engine
    )
    df_final = df_final.merge(
        contracts,
        how="left",
        left_on="contract_code",
        right_on="contract_code"
    )

    # Calcula tree_age usando pretty_tree_age
    df_final["tree_age"] = [
        pretty_tree_age(row["planting_date"], row["inventory_date"])
        for _, row in df_final.iterrows()
    ]
    # --- FIN DEL CÁLCULO ---

    # ---- LIMPIEZA DE SUFIJOS (_x, _y) ----
    for col in ['planting_year', 'planting_date']:
        y_col = f"{col}_y"
        x_col = f"{col}_x"
        if y_col in df_final.columns:
            df_final[col] = df_final[y_col]
        elif x_col in df_final.columns:
            df_final[col] = df_final[x_col]
        # Borra los duplicados
        for suffix in [y_col, x_col]:
            if suffix in df_final.columns:
                df_final.drop(suffix, axis=1, inplace=True)
    # ---- FIN LIMPIEZA DE SUFIJOS ----

    backup_table("inventory_metrics")
    df_final.to_sql("inventory_metrics", engine, schema="masterdatabase", if_exists="replace", index=False)
    print("✅ Métricas insertadas en masterdatabase.inventory_metrics")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--where', type=str, default="1=1", help="Filtro WHERE SQL, ej: \"region = 'Guatemala'\"")
    args = parser.parse_args()
    main(where=args.where)
