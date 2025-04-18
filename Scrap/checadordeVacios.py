from sqlalchemy import create_engine
import pandas as pd

# Conexión
conn_string = "postgresql+psycopg2://postgres:pauwlonia@localhost:5432/gisdb"
table_name = "US_InventoryDatabase_Q1_2025b"
contractos_extra = ['US0146', 'US0116', 'US0156', 'US0159']

# Motor SQL
engine = create_engine(conn_string)

# Lista para respaldo
backup_rows = []

# Revisión uno a uno
for code in contractos_extra:
    df = pd.read_sql(
        f'SELECT * FROM "{table_name}" WHERE "ContractCode" = %s',
        engine,
        params=(code,)  # ← cambio aquí
    )

    print(f"\n🧾 ContractCode: {code}")
    print(f"→ Registros: {len(df)}")

    # Evaluar si todas las columnas excepto ContractCode y FarmerName están vacías o nulas
    campos_utiles = df.drop(columns=["ContractCode", "FarmerName"], errors='ignore')
    if campos_utiles.dropna(how="all").empty:
        print("⚠️  Sin contenido útil. Puede eliminarse.")

        # Guardar en respaldo
        backup_rows.append(df)

        # Ejecutar eliminación
        with engine.begin() as conn:
            conn.execute(
                f'DELETE FROM "{table_name}" WHERE "ContractCode" = %s',
                [code]
            )
        print(f"🗑️  Registros de {code} eliminados.")
    else:
        print("✅ Tiene contenido. No se elimina.")

# Guardar respaldo si hay registros
if backup_rows:
    df_backup = pd.concat(backup_rows, ignore_index=True)
    df_backup.to_csv("backup_deleted_extras.csv", index=False)
    print("\n📁 Respaldo guardado en 'backup_deleted_extras.csv'")
