# inventory_catalog.py

from core.libs import pd
from core.db import get_engine
from Cruises.inventory_importer import save_inventory_to_sql, ensure_table


def create_inventory_catalog(df, engine, table_catalog_name):
    """
    Crea catálogo por ContractCode usando FarmerName desde extractors,
    y completa PlantingYear y TreesContract desde cat_farmers.
    Calcula TreesSampled directamente como el conteo por contrato.
    """
    # --- Paso 1: Validar columnas obligatorias ---
    required_cols = ["contractcode", "farmername", "cruisedate"]
    missing = [col for col in required_cols if col not in df.columns]

    if missing:
        raise KeyError(f"Columnas faltantes: {missing}. Verifica combine_files()")

    # --- Paso 2: Preparar columnas base ---
    cols_base = required_cols.copy()  # ["contractcode", "farmername", "cruisedate"]
    if "path" in df.columns:
        cols_base.insert(0, "path")  # Añadir "path" al inicio si existe

    # --- Paso 2.5: Filtrar filas basura (sin tree_number y status_id) ---
    for col in ["tree_number", "status_id"]:
        if col not in df.columns:
            df[col] = pd.NA

    mask_empty = df["tree_number"].isna() & df["status_id"].isna()
    if mask_empty.sum() > 0:
        print(f"🧹 Filtradas {mask_empty.sum()} filas vacías sin tree_number ni status_id")
        df_blanks = df[mask_empty]
        df_blanks.to_csv("filas_basura_catalog.csv", index=False)
    df = df[~mask_empty]

    # --- Paso 3: Crear DataFrame del catálogo ---
    try:
        df_catalog = df[cols_base].drop_duplicates(subset=["contractcode", "cruisedate"]).copy()
    except KeyError as e:
        print(f"❌ Error crítico: {str(e)}")
        print("Columnas disponibles en df:", df.columns.tolist())
        raise

    # --- Paso 4: Calcular TreesSampled ---
    sampled = df.groupby(["contractcode", "cruisedate"]).size().reset_index(name="TreesSampled")

    # --- Paso 5: Traer datos de cat_farmers ---
    query = '''
        SELECT 
            "contractcode", 
            "planting_year", 
            "treescontract" AS TreesContract 
        FROM cat_farmers
    '''
    # En inventory_catalog.py, antes del merge:
    df_farmers = pd.read_sql('SELECT "contractcode" AS contractcode, "farmername" FROM cat_farmers', engine)

    # --- Paso 6: Unir datos ---
    df_catalog = pd.merge(df_catalog, sampled, on=["contractcode", "cruisedate"], how="left")
    df_catalog = pd.merge(df_catalog, df_farmers, on="contractcode", how="left")

    # --- Paso 7: Ordenar columnas ---
    order = ["path", "contractcode", "farmername", "cruisedate", "planting_year", "trees_contract", "TreesSampled"]
    order = [col for col in order if col in df_catalog.columns]
    df_catalog = df_catalog[order]

    # --- Paso 7.5: Limpiar NaT para campos datetime ---
    for col in df_catalog.select_dtypes(include=["datetime64[ns]"]):
        df_catalog[col] = df_catalog[col].where(df_catalog[col].notna(), None)

    # --- Paso 8: Guardar en SQL ---
    ensure_table(
        df_catalog,
        engine,
        table_catalog_name,
        recreate=True
    )
    save_inventory_to_sql(df_catalog, engine, table_catalog_name, if_exists="replace")
    print(f"✅ Catálogo guardado: \n {table_catalog_name}")

    return df_catalog