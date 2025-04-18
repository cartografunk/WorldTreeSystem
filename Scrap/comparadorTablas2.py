import pandas as pd
from sqlalchemy import create_engine

 # Conexión
conn_string = "postgresql+psycopg2://postgres:pauwlonia@localhost:5432/gisdb"
engine = create_engine(conn_string)

 # Cargar datos filtrados por contrato
df_old = pd.read_sql("""
    SELECT * FROM public.inventory_us_2025
    WHERE "ContractCode" IN ('US0164', 'US0024')
""", engine)

df_new = pd.read_sql("""
    SELECT * FROM "US_InventoryDatabase_Q1_2025b"
    WHERE "ContractCode" IN ('US0164', 'US0024')
""", engine)

 # Crear clave única
def generar_serial(df):
    return (
        df["ContractCode"].astype(str) + "_" +
        df["Stand #"].astype(str).str.zfill(2) + "_" +
        df["Plot #"].astype(str).str.zfill(2) + "_" +
        df["Tree #"].astype(str).str.zfill(2)
    )

df_old["serial_key"] = generar_serial(df_old)
df_new["serial_key"] = generar_serial(df_new)

 # Comparación
faltan_en_nuevo = set(df_old["serial_key"]) - set(df_new["serial_key"])
faltan_en_viejo = set(df_new["serial_key"]) - set(df_old["serial_key"])

faltan_en_nuevo_df = df_old[df_old["serial_key"].isin(faltan_en_nuevo)]
faltan_en_viejo_df = df_new[df_new["serial_key"].isin(faltan_en_viejo)]

 # Mostrar conteos
print("📉 En el inventario viejo pero faltan en el nuevo:", len(faltan_en_nuevo_df))
print("📈 En el nuevo pero no estaban en el viejo:", len(faltan_en_viejo_df))

 # (Opcional) Exportar a Excel si quieres
faltan_en_nuevo_df.to_excel("faltan_en_nuevo.xlsx", index=False)
faltan_en_viejo_df.to_excel("faltan_en_viejo.xlsx", index=False)
