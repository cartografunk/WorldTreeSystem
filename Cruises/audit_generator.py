from utils.libs import pd, os
from utils.db import get_engine
from utils.cleaners import get_column
from utils.sql_helpers import prepare_df_for_sql  # ← importar la normalización SQL :contentReference[oaicite:0]{index=0}&#8203;:contentReference[oaicite:1]{index=1}
from inventory_importer import save_inventory_to_sql
from utils.sql_helpers_audit import prepare_audit_for_sql

def create_audit_table(engine, table_name: str, output_excel_folder=None):  # 👈 Recibir table_name
    country_code = table_name.split("_")[1].upper()  # Ej: "mx" → "MX"
    year = table_name.split("_")[2]                 # Ej: "2025a"
    inventory_table_name = table_name               # 👈 Usar el nombre real
    audit_table_name = f"audit_{country_code.lower()}_{year}"

    """
    Genera una tabla de auditoría a partir de datos de inventario y catálogos.
    - country_code: Código de país (ej: 'GT', 'US')
    - year: Año del inventario (ej: '2025')
    """
    # 1. Leer datos del catálogo de estados (cat_status)
    status_lookup = pd.read_sql("SELECT id, \"DeadTreeValue\", \"AliveTree\" FROM cat_status", engine)

    # Convertir IDs a enteros para evitar problemas de tipo float vs int
    status_lookup["id"] = status_lookup["id"].astype(int)

    # Crear diccionarios de mapeo
    map_dead = status_lookup.set_index("id")["DeadTreeValue"].to_dict()
    map_alive = status_lookup.set_index("id")["AliveTree"].to_dict()

    # 2. Leer tabla y aplicar esquema SQL
    df_inventory, _ = prepare_df_for_sql(
        pd.read_sql_table(f"inventory_{country_code.lower()}_{year}", engine)
    )

    # —————— PAUSA PARA DEPURAR ——————
    #print("\n▶︎ COLUMNAS EN df_inventory:", df_inventory.columns.tolist())
    # ————————————————————————————

    # 3. Normalizar nombres de columnas clave
    status_id_col = get_column(df_inventory, "status_id")  # Nombre real de la columna (str)
    contractcode_col = get_column(df_inventory, "ContractCode")
    tree_num_col = get_column(df_inventory, "Tree#")


    # 4. Validar existencia de columnas
    for col in [status_id_col, contractcode_col, tree_num_col]:
        if col not in df_inventory.columns:
            raise KeyError(f"❌ Columna crítica no encontrada: {col}")

    # 5. Calcular dead_tree y alive_tree desde cat_status
    #df_inventory["status_id"] = df_inventory[status_id_col].fillna(0).astype(int)  # Forzar enteros
    #df_inventory["dead_tree"] = df_inventory["status_id"].map(map_dead).fillna(1)  # Default muerto si no existe
    #df_inventory["alive_tree"] = df_inventory["status_id"].map(map_alive).fillna(0)

    # 6. Leer datos de agricultores (cat_farmers)
    df_farmers = pd.read_sql(
        'SELECT "ContractCode", "FarmerName", "PlantingYear", "#TreesContract" AS "Contracted_Trees" FROM cat_farmers',
        engine
    )
    df_farmers["ContractCode"] = df_farmers["ContractCode"].str.strip()

    # 7. Agrupar datos de inventario
    grouped = (df_inventory.groupby(contractcode_col, observed=True).agg(
        Total_Deads=("dead_tree", "sum"),
        Total_Alive=("alive_tree", "sum"),
        Trees_Sampled=(tree_num_col, "count")  # Contar árboles únicos por Tree #
    ).reset_index()
               )

    # 8. Combinar con datos de agricultores
    audit = pd.merge(
        df_farmers.rename(columns={"ContractCode": contractcode_col}),
        grouped,
        on=contractcode_col,
        how="inner"
    )

    # 9. Cálculos finales (manejo de división por cero)
    audit["Sample_Size"] = (audit["Trees_Sampled"] / audit["Contracted_Trees"].replace(0, 1)) 
    audit["Mortality"] = (audit["Total_Deads"] / audit["Trees_Sampled"].replace(0, 1)) 
    audit["Survival"] = (audit["Total_Alive"] / audit["Trees_Sampled"].replace(0, 1)) 
    audit["Remaining_Trees"] = audit["Contracted_Trees"] - audit["Total_Alive"]

    # 10. Formatear porcentajes
    for col in ["Sample_Size", "Mortality", "Survival"]:
        audit[col] = audit[col].round(1).astype(str) + "%"

    # 11. Renombrar columnas para reporte
    audit.rename(columns={
        contractcode_col: "Contract Code",
        "FarmerName": "Farmer Name",
        "PlantingYear": "Planting Year"
    }, inplace=True)

    # 12. Guardar resultados
    audit_table_name = f"audit_{country_code.lower()}_{year}"

    audit_sql, dtype = prepare_audit_for_sql(audit)  #  renombra + ordena + dtypes
    save_inventory_to_sql(
        audit_sql,
        engine,  # ← sigues pasando el Engine como connection_string
        audit_table_name,
        if_exists="replace",
        dtype=dtype  # ← **aquí va el dtype**
    )

    if output_excel_folder:
        output_path = os.path.join(output_excel_folder, f"{audit_table_name}.xlsx")
        audit.to_excel(output_path, index=False)

    return audit