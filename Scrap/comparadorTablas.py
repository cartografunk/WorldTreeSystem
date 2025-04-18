import pandas as pd
from sqlalchemy import create_engine

# Parámetros de conexión (ajusta según corresponda)
conn_string = "postgresql+psycopg2://postgres:pauwlonia@localhost:5432/gisdb"
engine = create_engine(conn_string)

# Leer las tablas en DataFrames
df_old = pd.read_sql("SELECT * FROM public.inventory_us_2025", engine)
df_new = pd.read_sql('SELECT * FROM "US_InventoryDatabase_Q1_2025b"', engine)

# Función para generar la clave única
def generar_serial(df):
    # Rellenar valores nulos y convertirlos a enteros (o strings)
    # Si algún campo no es numérico, ajusta el fillna (por ejemplo, '' para cadenas)
    # Asumiremos que Stand#, Plot# y Tree# deben ser números; en caso de NaN, usamos 0.
    df["Stand #"] = df["Stand #"].fillna(0).astype(int).astype(str).str.zfill(2)
    df["Plot #"]  = df["Plot #"].fillna(0).astype(int).astype(str).str.zfill(2)
    df["Tree #"]  = df["Tree #"].fillna(0).astype(int).astype(str).str.zfill(2)

    # Asegurarse de que ContractCode sea string, sin modificarlo
    df["ContractCode"] = df["ContractCode"].astype(str).str.strip()

    # Crear la clave única concatenando todo con guiones bajos.
    df["serial_key"] = df["ContractCode"] + "_" + df["Stand #"] + "_" + df["Plot #"] + "_" + df["Tree #"]
    return df


df_old = generar_serial(df_old)
df_new = generar_serial(df_new)

# Registros que están en el viejo pero faltan en el nuevo:
old_keys = set(df_old["serial_key"])
new_keys = set(df_new["serial_key"])

falta_old_new = old_keys - new_keys
falta_new_old = new_keys - old_keys

print("Registros presentes en el inventario antiguo pero faltan en el nuevo:")
print(f"Total: {len(falta_old_new)}")
print(falta_old_new)

print("\nRegistros presentes en el inventario nuevo pero faltan en el antiguo:")
print(f"Total: {len(falta_new_old)}")
print(falta_new_old)


import pandas as pd
from sqlalchemy import create_engine

# Cadena de conexión
conn_string = "postgresql+psycopg2://postgres:pauwlonia@localhost:5432/gisdb"
engine = create_engine(conn_string)

# Leer tablas desde la base de datos
df_old = pd.read_sql("SELECT * FROM public.inventory_us_2025", engine)
df_new = pd.read_sql('SELECT * FROM "US_InventoryDatabase_Q1_2025b"', engine)

# Contar registros por ContractCode
old_counts = df_old.groupby("ContractCode").size().reset_index(name="count_old")
new_counts = df_new.groupby("ContractCode").size().reset_index(name="count_new")

# Combinar y calcular diferencias
comparison_df = pd.merge(old_counts, new_counts, on="ContractCode", how="outer").fillna(0)
comparison_df["count_old"] = comparison_df["count_old"].astype(int)
comparison_df["count_new"] = comparison_df["count_new"].astype(int)
comparison_df["diff"] = comparison_df["count_new"] - comparison_df["count_old"]

# Ordenar por diferencia
comparison_df = comparison_df.sort_values(by="diff", ascending=False)

# Mostrar diferencias significativas
print("🔍 Diferencias encontradas:")
print(comparison_df[comparison_df["diff"] != 0])

# Exportar resultado
comparison_df.to_excel("Comparativo_ContractCode.xlsx", index=False)
print("✅ Archivo 'Comparativo_ContractCode.xlsx' generado correctamente.")
