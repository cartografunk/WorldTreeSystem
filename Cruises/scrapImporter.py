from core.libs import pd
from utils.db import get_engine
from inventory_importer import save_inventory_to_sql

# Ruta al CSV corregido
csv_path = r"C:\Users\HeyCe\World Tree Technologies Inc\Forest Inventory - Documentos\Guatemala\2024_ForestInventoryQ1_25\x Scrap\GT0045a_Trapiche de la Vega.csv"

# Leer CSV
df = pd.read_csv(csv_path)

# Limpiar espacios en nombres de columnas
df.columns = df.columns.str.strip()

# Eliminar columnas completamente vacías (por si hay de más)
df = df.dropna(axis=1, how="all")

# Forzar que ContractCode esté rellenado
df["ContractCode"] = "GT0045"

# Agregar metadata (puedes cambiar si lo sabes)
df["FarmerName"] = "Trapiche de la Vega Sociedad Anónima / Walesca María Isabel Agüero Urruela de Maegli"
df["CruiseDate"] = pd.to_datetime("2025-01-15")

# Conexión y guardado
engine = get_engine()
save_inventory_to_sql(df, engine, "inventory_gt_2025", if_exists="append")

print("✅ CSV manual de GT0045 agregado a inventario_gt_2025 con éxito.")
