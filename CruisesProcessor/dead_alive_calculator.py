#CruisesProcessor/dead_alive_calculator.py

from core.libs import pd, text

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

        # 3️⃣ Detectar nuevos Status
        catalog_values = set(cat["nombre"].dropna().unique())
        status_values = set(df["Status"].dropna().unique())
        missing_statuses = sorted(status_values - catalog_values)
        if missing_statuses:
            print(f"⚠️ Hay {len(missing_statuses)} Status nuevos no presentes en cat_status:")
            for val in missing_statuses:
                print(f"   - '{val}'")

            # Insertar automáticamente
            with engine.begin() as conn:
                for val in missing_statuses:
                    conn.execute(
                        text("INSERT INTO cat_status (nombre) VALUES (:val) ON CONFLICT DO NOTHING"),
                        {"val": val}
                    )
            print("✅ Nuevos Status añadidos a cat_status. Por favor completa los campos 'DeadTreeValue' y 'AliveTree'.")

            # Pausar para edición manual
            resp = input("⏸️ Pausado. ¿Ya actualizaste los valores en la base de datos? (y/n): ").strip().lower()
            if resp != "y":
                raise RuntimeError("⛔ Proceso detenido por el usuario para edición manual de cat_status.")

            # Recargar catálogo ya actualizado
            cat = pd.read_sql("""
                SELECT id, nombre, "DeadTreeValue" AS dead, "AliveTree" AS alive
                FROM cat_status
            """, engine)
            mapping_dead = {
                row["nombre"]: int(row["dead"]) if not pd.isna(row["dead"]) else 0
                for _, row in cat.iterrows() if pd.notna(row["nombre"])
            }
            mapping_alive = {
                row["nombre"]: int(row["alive"]) if not pd.isna(row["alive"]) else 0
                for _, row in cat.iterrows() if pd.notna(row["nombre"])
            }

        # 4️⃣ Mapear
        df["dead_tree"] = df["Status"].map(mapping_dead).fillna(0).astype(int)
        df["alive_tree"] = df["Status"].map(mapping_alive).fillna(0).astype(int)

    return df
