# dead_alive_calculator.py

from core.libs import pd
import re

def calculate_dead_alive(df: pd.DataFrame, engine) -> pd.DataFrame:
    """
    Agrega las columnas 'dead_tree' y 'alive_tree' mapeando el texto de 'status'
    (por ejemplo "1) Viva/Vivo", "Vivo", "Live", "Dead", etc.) contra los valores
    de cat_status. Primero normalizamos ambas cadenas (de cat_status y de df["status"])
    para que coincidan (quitando prefijos numéricos, espacios y pasándolo a mayúsculas).
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

    # 2) Construir un diccionario que asocie cada variante de texto a su dead/alive
    def normalize_text(txt: str) -> str:
        #  - convertir a str (por si hay NaN)
        #  - quitar prefijo numérico tipo "1) " o "14) "
        #  - quitar espacios extras al inicio/final
        #  - pasar a mayúsculas
        if pd.isna(txt):
            return ""
        s = str(txt).strip()
        # eliminar prefijo numérico "n) " con regex
        s = re.sub(r"^\d+\)\s*", "", s)
        return s.strip().upper()

    # Para cada fila de cat_status, normalizamos tanto 'nombre' como 'nombre_en' y
    # mapeamos a su id, dead, alive
    mapping_dead = {}
    mapping_alive = {}
    for _, row in cat.iterrows():
        clave_es = normalize_text(row["nombre"])
        clave_en = normalize_text(row["nombre_en"])
        dead_val = int(row["dead"]) if not pd.isna(row["dead"]) else 0
        alive_val = int(row["alive"]) if not pd.isna(row["alive"]) else 0

        if clave_es:
            mapping_dead[clave_es] = dead_val
            mapping_alive[clave_es] = alive_val
        if clave_en:
            mapping_dead[clave_en] = dead_val
            mapping_alive[clave_en] = alive_val

    # 3) Normalizar la columna 'status' del DataFrame
    series_status_norm = (
        df["status"]
        .astype(str)
        .apply(normalize_text)
    )

    # 4) Mapear texto normalizado a dead_tree y alive_tree
    df["dead_tree"]  = series_status_norm.map(mapping_dead).fillna(0).astype(int)
    df["alive_tree"] = series_status_norm.map(mapping_alive).fillna(0).astype(int)

    return df
