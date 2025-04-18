# inventory_catalog.py

from utils.libs import pd
from utils.db import get_engine
from inventory_importer import save_inventory_to_sql


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

    # --- Paso 3: Crear DataFrame del catálogo ---
    try:
        df_catalog = df[cols_base].drop_duplicates(subset=["contractcode"]).copy()
    except KeyError as e:
        print(f"❌ Error crítico: {str(e)}")
        print("Columnas disponibles en df:", df.columns.tolist())
        raise

    # --- Paso 4: Calcular TreesSampled ---
    sampled = df.groupby("contractcode").size().reset_index(name="TreesSampled")

    # --- Paso 5: Traer datos de cat_farmers ---
    query = '''
        SELECT 
            "ContractCode", 
            "PlantingYear", 
            "#TreesContract" AS TreesContract 
        FROM cat_farmers
    '''
    # En inventory_catalog.py, antes del merge:
    df_farmers = pd.read_sql('SELECT "ContractCode" AS contractcode, "FarmerName" FROM cat_farmers', engine)

    # --- Paso 6: Unir datos ---
    df_catalog = pd.merge(df_catalog, df_farmers, on="contractcode", how="left")

    # --- Paso 7: Ordenar columnas ---
    order = ["path", "ContractCode", "farmername", "cruisedate", "PlantingYear", "TreesContract", "TreesSampled"]
    order = [col for col in order if col in df_catalog.columns]
    df_catalog = df_catalog[order]

    # --- Paso 8: Guardar en SQL ---
    save_inventory_to_sql(df_catalog, engine, table_catalog_name, if_exists="replace")
    print(f"✅ Catálogo guardado: {table_catalog_name}")

    return df_catalog