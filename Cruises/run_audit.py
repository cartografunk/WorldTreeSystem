from pathlib import Path
from core.libs import pd
from utils.metadata_extractor import extract_metadata_from_folder
from core.schema import rename_columns_using_schema
from Cruises.general_importer import prepare_df_for_sql, ensure_table, save_inventory_to_sql

def create_audit_table(engine, inventory_table_name, folder=None):
    # Leer tabla base
    df = pd.read_sql_table(inventory_table_name, engine)

    # Unir con status
    df_status = pd.read_sql('SELECT id, "DeadTreeValue", "AliveTree" FROM cat_status', engine)
    df_merged = pd.merge(df, df_status, left_on="status_id", right_on="id", how="left")

    # Agrupar por contrato
    grouped = df_merged.groupby("contractcode").agg(
        total_deads=("dead_tree", "sum"),
        total_alive=("alive_tree", "sum"),
        trees_sampled=("contractcode", "count")
    ).reset_index()

    # Unir con datos del productor
    df_farmers = pd.read_sql(
        'SELECT contract_code, farmer_name, planting_year, "#TreesContract" FROM cat_farmers',
        engine
    )
    df_farmers.rename(columns={"#TreesContract": "contracted_trees"}, inplace=True)

    audit = pd.merge(df_farmers, grouped, left_on="contract_code", right_on="contractcode", how="inner")
    audit.fillna({"total_deads": 0, "total_alive": 0, "trees_sampled": 0}, inplace=True)

    # Cálculo de métricas
    audit["sample_size"] = (audit["trees_sampled"] / audit["contracted_trees"]).fillna(0)
    audit["mortality"] = (audit["total_deads"] / audit["trees_sampled"]).replace([float("inf"), float("nan")], 0)
    audit["survival"] = (audit["total_alive"] / audit["trees_sampled"]).replace([float("inf"), float("nan")], 0)
    audit["remaining_trees"] = audit["contracted_trees"] - audit["total_alive"]

    # Formato de % como texto
    audit["sample_size"] = (audit["sample_size"] * 100).round(1).astype(str) + "%"
    audit["mortality"] = (audit["mortality"] * 100).round(1).astype(str) + "%"
    audit["survival"] = (audit["survival"] * 100).round(1).astype(str) + "%"

    # Renombrar columnas con schema
    audit = rename_columns_using_schema(audit)

    # Preparar para SQL
    df_sql, dtype_for_sql = prepare_df_for_sql(audit)

    # Año desde metadata
    metadata = extract_metadata_from_folder(folder)
    cruise_date = metadata.get("cruise_date")
    if not cruise_date:
        raise ValueError("CruiseDate no encontrado en metadata.")
    year = cruise_date.year

    # Nombre de tabla dinámico
    country_code = inventory_table_name.split("_")[1].lower()
    table_name = f"audit_{country_code}_{year}"

    # Crear y guardar tabla
    ensure_table(df_sql, table_name, engine, dtype_for_sql, overwrite=True)
    save_inventory_to_sql(df_sql, table_name, engine, dtype_for_sql)

    # Exportar a Excel si hay folder
    if folder:
        output_path = Path(folder) / f"{table_name}.xlsx"
        df_sql.to_excel(output_path, index=False)
        print(f"📁 Exportada a Excel: {output_path}")

    print(f"✅ Auditoría guardada en SQL: {table_name}")



if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Genera tabla de auditoría por contrato.")
    parser.add_argument("--table_name", required=True)
    parser.add_argument("--output_folder", default=None)

    args = parser.parse_args()
    engine = get_engine()
    create_audit_table(engine, args.table_name, args.output_folder)
