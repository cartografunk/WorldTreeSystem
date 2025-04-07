import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from fpdf import FPDF
import os

# Configuración de conexión a PostgreSQL
DB_NAME = "gisdb"
DB_USER = "postgres"
DB_PASSWORD = "pauwlonia"
DB_HOST = "localhost"
DB_PORT = "5432"

# Ruta de salida
output_folder = r"C:\Users\HeyCe\World Tree Technologies Inc\Forest Inventory - Documentos\Inventory_DataBase_Processed"
excel_path = os.path.join(output_folder, "Resumen_Inventario_2025.xlsx")
pdf_path = os.path.join(output_folder, "Resumen_Inventario_2025.pdf")

# Conectar a PostgreSQL
engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Consulta SQL para resumen de inventario
query = """
SELECT
    inv."Contract Code" AS contract_code,
    MIN(inv."Farmer Name") AS farmer_name,
    MIN(inv."Planting Year") AS planting_year,
    COUNT(*) AS trees_sampled,
    SUM(inv.dead_tree) AS total_deads,
    SUM(inv.alive_tree) AS total_alive,
    ROUND(SUM(inv.dead_tree)::numeric / COUNT(*) * 100, 1) AS mortality,
    ROUND(SUM(inv.alive_tree)::numeric / COUNT(*) * 100, 1) AS survivability,
    cf.contracted_trees,
    (cf.contracted_trees - SUM(inv.alive_tree)) AS remaining_trees
FROM
    public.cr_inventory_2025 inv
JOIN
    public.cat_farmerscomplete cf
ON
    inv."Contract Code" = cf.contract_code
GROUP BY
    inv."Contract Code", cf.contracted_trees
ORDER BY
    inv."Contract Code";

"""

# Ejecutar la consulta
df = pd.read_sql_query(query, engine)

# Guardar a Excel
df.to_excel(excel_path, index=False)

# Crear gráfica de barras
plt.figure(figsize=(10, 6))
plt.bar(df['contract_code'], df['mortality'], label='Mortality %')
plt.bar(df['contract_code'], df['survivability'], bottom=df['mortality'], label='Survivability %')
plt.ylabel('Percentage')
plt.title('Mortality vs Survivability por Contrato')
plt.xticks(rotation=45, ha='right')
plt.legend()
plt.tight_layout()

chart_path = os.path.join(output_folder, "mortality_chart.png")
plt.savefig(chart_path)
plt.close()

# Crear PDF
pdf = FPDF()
pdf.add_page()
pdf.set_font("Arial", "B", 12)
pdf.cell(200, 10, "Resumen de Inventario 2025", ln=True, align="C")
pdf.ln(5)
pdf.set_font("Arial", "", 10)

# Insertar resumen por contrato
for index, row in df.iterrows():
    resumen = (
        f"{row['contract_code']}: {row['trees_sampled']} árboles, "
        f"{row['total_alive']} vivos ({row['survivability']}%), "
        f"{row['total_deads']} muertos ({row['mortality']}%)"
    )
    pdf.cell(0, 10, resumen, ln=True)

# Insertar gráfica
pdf.image(chart_path, x=10, y=pdf.get_y() + 5, w=180)

# Guardar PDF
pdf.output(pdf_path)

print("✅ Reportes generados correctamente.")
