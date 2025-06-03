# — imports al inicio del archivo —
from core.schema import rename_columns_using_schema
from Cruises.utils.sql_helpers import prepare_df_for_sql
from core.schema import get_column
from Cruises.inventory_importer import save_inventory_to_sql, ensure_table
from core.libs import pd, os

def create_audit_table(engine, table_name: str, output_excel_folder=None):
    country_code = table_name.split("_")[1].upper()
    year = table_name.split("_")[2]
    inventory_table_name = table_name
    audit_table_name = f"audit_{country_code.lower()}_{year}"

    # 1. Leer e renombrar inventario
    df_inventory_raw = pd.read_sql_table(f"inventory_{country_code.lower()}_{year}", engine)
    df_inventory = rename_columns_using_schema(df_inventory_raw)

    # 2. Leer y renombrar farmers
    df_farmers_raw = pd.read_sql(
        'SELECT contractcode, farmername, planting_year, contracted_trees FROM cat_farmers',
        engine
    )

    # 3. Obtener los nombres de columna normalizados
    contractcode_col = get_column(df_inventory, "contractcode")
    tree_num_col    = get_column(df_inventory, "tree_number")

    # 4. Agrupar inventario
    grouped = df_inventory.groupby(contractcode_col, observed=True).agg(
        total_deads  =("dead_tree",  "sum"),
        total_alive  =("alive_tree", "sum"),
        trees_sampled=(tree_num_col,"count")
    ).reset_index()

    # 5. Merge inventario + farmers
    audit = pd.merge(df_farmers_raw, grouped, on="contractcode", how="inner")

    # 6. Cálculos y formateo
    audit["sample_size"]    = audit["trees_sampled"] / audit["contracted_trees"].replace(0,1)
    audit["mortality"]      = audit["total_deads"]   / audit["trees_sampled"].replace(0,1)
    audit["survival"]       = audit["total_alive"]   / audit["trees_sampled"].replace(0,1)
    audit["remaining_trees"]= audit["contracted_trees"] - audit["total_alive"]

    for col in ["sample_size","mortality","survival"]:
        audit[col] = (audit[col] * 100).round(1).astype(str) + "%"

    # 6a. Crear id secuencial para clave primaria
    audit = audit.reset_index(drop=True)
    audit.insert(0, "id", audit.index + 1)

    # 7. Preparar e insertar
    audit = rename_columns_using_schema(audit)
    audit_sql, dtype = prepare_df_for_sql(audit)
    ensure_table(
        audit_sql,  # 1. el DataFrame
        engine,  # 2. la instancia Engine
        audit_table_name,  # 3. el nombre de la tabla
        recreate=True  # opcionalmente fuerza recreación
    )

    save_inventory_to_sql(audit_sql, engine, audit_table_name, if_exists="append", dtype=dtype)

    # 8. Exportar a Excel si corresponde
    if output_excel_folder:
        output_path = os.path.join(output_excel_folder, f"{audit_table_name}.xlsx")
        audit_sql.to_excel(output_path, index=False)
        print(f"📁 Excel exportado en: \n {output_path}")

    print(f"✅ Auditoría completada: \n {audit_table_name}")
    return audit
