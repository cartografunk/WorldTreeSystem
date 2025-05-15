# dead_alive_calculator.py

from core.libs import pd

def calculate_dead_alive(df: pd.DataFrame, engine) -> pd.DataFrame:
    """
    Agrega las columnas 'dead_tree' y 'alive_tree' mapeando status_id contra cat_status.
    """
    # Leer cat_status solo una vez
    status_lookup = pd.read_sql(
        'SELECT id, "DeadTreeValue", "AliveTree" FROM cat_status',
        engine
    )
    # Aseguramos enteros
    status_lookup["id"] = status_lookup["id"].astype(int)

    map_dead  = status_lookup.set_index("id")["DeadTreeValue"].to_dict()
    map_alive = status_lookup.set_index("id")["AliveTree"].to_dict()

    df["dead_tree"]  = df["status_id"].map(map_dead).fillna(0).astype(int)
    df["alive_tree"] = df["status_id"].map(map_alive).fillna(0).astype(int)
    return df
