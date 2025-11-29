# inventory_metrics/generate.py
"""
✅ SAFE VERSION: Usa to_sql_with_backup en lugar de to_sql directo
Ya tenía backup_table(), ahora usa el wrapper completo
"""

from tqdm import tqdm
from InventoryMetrics.processing_metrics import aggregate_contracts
from InventoryMetrics.generate_helpers import safe_numeric
from InventoryMetrics.inventory_retriever import get_inventory_tables, get_cruise_date
from InventoryMetrics.generate_helpers import clean_and_fuse_metrics

from core.libs import re, pd, text, argparse
from core.db import get_engine
from core.schema_helpers import get_column
from core.safe_ops import to_sql_with_backup  # ✅ NUEVO: Reemplaza to_sql normal

from InventoryMetrics.generate_helpers import create_cat_inventory_tables
from InventoryMetrics.inventory_retriever import get_inventory_tables
from InventoryMetrics.planting_times import pretty_tree_age


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

    # ✅ CAMBIO CRÍTICO: Usa to_sql_with_backup en lugar de backup_table + to_sql
    # CÓDIGO ANTERIOR:
    # backup_table("inventory_metrics")
    # df_final.to_sql("inventory_metrics", engine, schema="masterdatabase", if_exists="replace", index=False)

    # CÓDIGO NUEVO (SAFE):
    print("🛡️  Guardando métricas con protección de backup...")
    to_sql_with_backup(
        df_final,
        engine,
        "inventory_metrics",
        schema="masterdatabase",
        if_exists="replace",
        index=False
    )
    print("✅ Métricas insertadas en masterdatabase.inventory_metrics")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--where', type=str, default="1=1", help="Filtro WHERE SQL, ej: \"region = 'Guatemala'\"")
    args = parser.parse_args()
    main(where=args.where)