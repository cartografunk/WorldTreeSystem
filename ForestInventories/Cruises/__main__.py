#!/usr/bin/env python

from utils.libs import argparse, os, pd, re, unicodedata
from utils.cleaners import clean_cruise_dataframe, standardize_units, get_column
from utils.db import get_engine
from catalog_normalizer import normalize_catalogs
from union import combine_files
from filters import create_filter_func
from inventory_importer import save_inventory_to_sql
from inventory_catalog import create_inventory_catalog
#from audit_generator import create_audit_table
#from utils.sql_helpers import prepare_df_for_sql

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

    # ———————————————————————————————————————
    # Prepara nombre y carpeta para auditoría
    match = re.search(r'inventory_(\w{2})_(\d{4})', args.table_name.lower())

    if not match:
            raise ValueError("❌ 'table_name' no sigue el patrón: inventory_<país>_<año>")
    country_code_audit = match.group(1)
    year_audit = match.group(2)
    output_folder = os.path.dirname(args.output_file)
    

    #  A) Cargar catálogo de agricultores
    df_farmers = pd.read_sql(
        'SELECT "ContractCode" AS contractcode, '
            '"FarmerName", "PlantingYear", "#TreesContract" AS Contracted_Trees '
        'FROM cat_farmers',
        engine
    )
    df_farmers["contractcode"] = df_farmers["contractcode"].str.strip()

    #  B) Agrupar directamente en el DataFrame completo
    audit = (
        df_combined
        .groupby("contractcode", observed=True)
        .agg(
            Total_Deads=("dead_tree", "sum"),
            Total_Alive=("alive_tree", "sum"),
            Trees_Sampled=("tree_number", "count")
        )
        .reset_index()
        .merge(df_farmers, on="contractcode", how="inner")
    )

    print(">>> df_combined columnas:", df_combined.columns.tolist())
    print(">>> Primeras filas de df_combined:")
    print(df_combined[['contractcode', 'tree_number', 'dead_tree', 'alive_tree']].head(5))

    # —————— Prueba del groupby ——————
    audit_test = (
        df_combined
        .groupby("contractcode", observed=True)
        .agg(
            Total_Deads=("dead_tree", "sum"),
            Total_Alive=("alive_tree", "sum"),
            Trees_Sampled=("tree_number", "count")
        )
        .reset_index()
    )
    print(">>> Resultado audit_test:")
    print(audit_test.head(5))

    #  C) Calcular métricas y formatear
    audit["Sample_Size"] = ((audit["Trees_Sampled"] / audit["Contracted_Trees"].replace(0, 1)) * 100).round(1).astype(
        str) + "%"
    audit["Mortality"] = ((audit["Total_Deads"] / audit["Trees_Sampled"].replace(0, 1)) * 100).round(1).astype(
        str) + "%"
    audit["Survival"] = ((audit["Total_Alive"] / audit["Trees_Sampled"].replace(0, 1)) * 100).round(1).astype(str) + "%"
    audit["Remaining_Trees"] = audit["Contracted_Trees"] - audit["Total_Alive"]

    #  D) Renombrar columnas a estilo report
    audit = audit.rename(columns={
        "contractcode": "Contract Code",
        "FarmerName": "Farmer Name",
        "PlantingYear": "Planting Year"
    })

    #  E) Guardar auditoría en SQL y Excel
    audit_table = f"audit_{country_code_audit}_{year_audit}"
    save_inventory_to_sql(audit, engine, audit_table, if_exists="replace")
    if output_folder:
        audit.to_excel(os.path.join(output_folder, f"{audit_table}.xlsx"), index=False)

    # F) Guardar inventario final en SQL y catálogo
    df_sql, dtype_for_sql = prepare_df_for_sql(df_combined)
    save_inventory_to_sql(df_sql, engine, args.table_name, if_exists="replace", dtype=dtype_for_sql)
    create_inventory_catalog(df_combined, engine, f"cat_{args.table_name}")

    print("✅ Proceso completado.")


if __name__ == "__main__":
    main()
