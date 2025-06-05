# dead_alive_calculator.py

from core.libs import pd

def calculate_dead_alive(df: pd.DataFrame, engine) -> pd.DataFrame:
    """
    Agrega las columnas 'dead_tree' y 'alive_tree' mapeando el texto de 'status'
    contra los valores de cat_status.
    """
    # 1) Leer cat_status completo con nombre y nombre_en
    sql = """
        SELECT
            id,
            nombre,
            nombre_en,
            "DeadTreeValue" AS dead,
            "AliveTree"   AS alive
        FROM cat_status
    """
    cat = pd.read_sql(sql, engine)

    # 2) Construir los diccionarios mapping_dead / mapping_alive
    mapping_dead = {}
    mapping_alive = {}
    for _, row in cat.iterrows():
        clave_es = row["nombre"]
        clave_en = row["nombre_en"]
        dead_val = int(row["dead"]) if not pd.isna(row["dead"]) else 0
        alive_val = int(row["alive"]) if not pd.isna(row["alive"]) else 0

        if clave_es and clave_es not in mapping_dead:
            mapping_dead[clave_es] = dead_val
            mapping_alive[clave_es] = alive_val
            print("Se añadieron registros en cat_status")
        if clave_en and clave_en not in mapping_dead:
            mapping_dead[clave_en] = dead_val
            mapping_alive[clave_en] = alive_val

    # 4) Mapear texto de 'status' usando pandas.Series.map
    df["dead_tree"]  = df["Status"].map(mapping_dead).fillna(0).astype(int)
    df["alive_tree"] = df["Status"].map(mapping_alive).fillna(0).astype(int)

    return df
