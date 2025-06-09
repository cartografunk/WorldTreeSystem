# dead_alive_calculator.py

from core.libs import pd

def calculate_dead_alive(df: pd.DataFrame, engine) -> pd.DataFrame:
    """
    Agrega las columnas 'dead_tree' y 'alive_tree' mapeando el texto de 'Status'
    contra los valores de cat_status.
    También avisa si hay statuses nuevos que no están en el catálogo.
    """
    # 1) Leer cat_status completo con nombre y nombre_en
    sql = """
        SELECT
            id,
            nombre,
            "DeadTreeValue" AS dead,
            "AliveTree" AS alive
        FROM cat_status
    """
    cat = pd.read_sql(sql, engine)

    # 2) Construir los diccionarios mapping_dead / mapping_alive para ambos idiomas
    mapping_dead = {}
    mapping_alive = {}
    for _, row in cat.iterrows():
        for clave in [row["nombre"]]:
            if pd.notna(clave) and clave not in mapping_dead:
                mapping_dead[clave] = int(row["dead"]) if not pd.isna(row["dead"]) else 0
                mapping_alive[clave] = int(row["alive"]) if not pd.isna(row["alive"]) else 0

    # 3) Validar si hay nuevos statuses en el DataFrame que no estén en el catálogo
    status_values = set(df["Status"].dropna().unique())
    missing_statuses = status_values - set(mapping_dead.keys())
    if missing_statuses:
        print("⚠️ Hay status en el DataFrame que NO están en cat_status:")
        for missing in missing_statuses:
            print(f"   - '{missing}'")

    # 4) Mapear texto de 'Status' usando pandas.Series.map
    df["dead_tree"]  = df["Status"].map(mapping_dead).fillna(0).astype(int)
    df["alive_tree"] = df["Status"].map(mapping_alive).fillna(0).astype(int)

    return df
