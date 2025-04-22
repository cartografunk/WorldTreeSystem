#!/usr/bin/env python

from utils.libs import argparse, pd
from utils.cleaners import clean_cruise_dataframe, standardize_units, get_column
from utils.db import get_engine
from catalog_normalizer import normalize_catalogs
from union import combine_files
from filters import create_filter_func
from inventory_importer import save_inventory_to_sql
from inventory_catalog import create_inventory_catalog
from Cruises.audit_pipeline import run_audit


def main():
    parser = argparse.ArgumentParser(
        description="Procesa inventario forestal: combina, limpia, normaliza y guarda en SQL."
    )
    parser.add_argument("--cruises_path", required=True)
    parser.add_argument("--output_file", required=True)
    parser.add_argument("--allowed_codes", nargs="+", default=None)
    parser.add_argument("--table_name", required=True,
                        help="Nombre de la tabla SQL en la que se almacenará el inventario, por ejemplo 'inventory_gt_2025'.")
    parser.add_argument("--country_code", required=True, help="Código de país, por ejemplo GT, US, etc.")

    args = parser.parse_args()

    # Filtro
    filter_func = None
    if args.allowed_codes and args.allowed_codes != ["ALL"]:
        filter_func = create_filter_func(args.allowed_codes)

    print("📂 Procesando censos en:", args.cruises_path)
    df_combined = combine_files(args.cruises_path, filter_func=filter_func)
    if df_combined is None or df_combined.empty:
        print("⚠️ No se encontraron datos combinados.")
        return

    # Limpieza y unidades
    df_combined = clean_cruise_dataframe(df_combined)
    df_combined = standardize_units(df_combined)

    # Cálculo de Doyle
    dbh_col = get_column(df_combined, "DBH (in)")
    tht_col = get_column(df_combined, "THT (ft)")
    df_combined["DBH (in)"] = pd.to_numeric(dbh_col, errors="coerce")
    df_combined["THT (ft)"] = pd.to_numeric(tht_col, errors="coerce")
    df_combined["doyle_bf"] = ((df_combined["DBH (in)"] - 4) ** 2) * (df_combined["THT (ft)"] / 16)

    #Obtener engine
    engine = get_engine()

    # 🧠 Normalizar campos a catálogos
    catalog_columns = {
        'Species': 'cat_species',
        'Defect': 'cat_defect',
        'Pests': 'cat_pest',
        'Disease': 'cat_disease',
        'Coppiced': 'cat_coppiced',
        'Permanent Plot': 'cat_permanent_plot',
        'Status': 'cat_status'
    }
    df_combined = normalize_catalogs(df_combined, engine, catalog_columns, country_code=args.country_code)

    # 🔁 Agregar columnas dead_tree y alive_tree según cat_status
    status_lookup = pd.read_sql('SELECT id, "DeadTreeValue", "AliveTree" FROM cat_status', engine)
    map_dead = status_lookup.set_index("id")["DeadTreeValue"].to_dict()
    map_alive = status_lookup.set_index("id")["AliveTree"].to_dict()

    df_combined["dead_tree"] = df_combined["status_id"].map(map_dead).fillna(0)
    df_combined["alive_tree"] = df_combined["status_id"].map(map_alive).fillna(0)

    # 💾 Exportar Excel combinado
    df_combined.to_excel(args.output_file, index=False)
    print(f"✅ Archivo combinado guardado en: {args.output_file}")

    # -------------- DESPUÉS de grabar el inventario -----------------
    df_sql, dtype_for_sql = prepare_df_for_sql(df_combined)
    save_inventory_to_sql(df_sql, engine, args.table_name,
                          if_exists="replace", dtype=dtype_for_sql)

    # -------------- Lanzar auditoría -----------------
    run_audit(engine, args.table_name, args.output_file)
    # -------------------------------------------------

    create_inventory_catalog(df_combined, engine, f"cat_{args.table_name}")
    print("✅ Proceso completado.")


if __name__ == "__main__":
    main()
