import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
import os

# Conexión
engine = create_engine("postgresql://postgres:pauwlonia@localhost:5432/helloworldtree")

# Consulta SQL
query = """
SELECT inv."DBH (in)" AS dbh,
       inv."Merch. HT (ft)" AS mht,
       inv."Contract Code" AS "ContractCode",
       f."FarmerName"
FROM public.inventory_us_2025 AS inv
JOIN public.cat_farmers AS f ON inv."Contract Code" = f."ContractCode"
JOIN public.cat_status AS s ON inv."status_id" = s."id"
WHERE f."PlantingYear" = 2016
  AND s."AliveTree" = 1
  AND inv."DBH (in)" > 0
  AND inv."Merch. HT (ft)" > 0
"""

df = pd.read_sql(query, engine)

# Crear columna de leyenda personalizada
df["Contract Info"] = df["ContractCode"] + " – " + df["FarmerName"]

# Carpeta de salida
output_folder = r"C:\Users\HeyCe\World Tree Technologies Inc\Forest Inventory - Documentos\USA\Reports\MHT x DBH US 2016"
os.makedirs(output_folder, exist_ok=True)

# Constantes visuales
limite_dbh = 8
limite_mht = 6

# Obtener máximos globales
max_mht2 = df["mht"].max()
max_dbh2 = df["dbh"].max()


# Loop por contrato
for contract, group in df.groupby("ContractCode"):
    farmer = group["FarmerName"].iloc[0]
    contract_info = f"{contract} – {farmer}"
    group["Contract Info"] = contract_info

    # Conteo de árboles por zona
    count_undesirable = group[(group["dbh"] < 8) | (group["mht"] < 6)].shape[0]
    count_suboptimal = group[
        (group["dbh"] >= 8) & (group["dbh"] <= 10) &
        (group["mht"] >= 6) & (group["mht"] <= 10)
    ].shape[0]

    fig = px.scatter(
        group,
        x="mht",
        y="dbh",
        color="Contract Info",
        hover_name="FarmerName",
        labels={"mht": "Merchantable Height (ft)", "dbh": "DBH (in)", "Contract Info": "Contract Info"},
        title=f"DBH vs MHT – {contract_info}",
        opacity=0.8
    )

    # 🟥 Zona roja por DBH < 8
    fig.add_shape(type="rect", x0=0, x1=max_mht2, y0=0, y1=8,
                  fillcolor="red", opacity=0.2, layer="below", line_width=0)

    # 🟥 Zona roja por MHT < 6
    fig.add_shape(type="rect", x0=0, x1=6, y0=0, y1=max_dbh2,
                  fillcolor="red", opacity=0.2, layer="below", line_width=0)

# 🟨 Zona amarilla por DBH < 10
    fig.add_shape(type="rect", x0=6, x1=max_mht2, y0=8, y1=10,
                  fillcolor="yellow", opacity=0.2, layer="below", line_width=0)

    # 🟨 Zona amarilla: MHT 8
    fig.add_shape(type="rect", x0=6, x1=8, y0=8, y1=max_dbh2,
                  fillcolor="yellow", opacity=0.2, layer="below", line_width=0)

    # Líneas guía rojas
    fig.add_shape(type="line", x0=6, x1=6, y0=0, y1=max_dbh2,
                  line=dict(color="red", dash="dash", width=2))
    fig.add_shape(type="line", x0=0, x1=max_mht2, y0=8, y1=8,
                  line=dict(color="red", dash="dash", width=2))

    # Líneas guía amarillas
    fig.add_shape(type="line", x0=8, x1=8, y0=0, y1=max_dbh2,
                  line=dict(color="gold", dash="dash", width=1.5))
    fig.add_shape(type="line", x0=0, x1=max_mht2, y0=10, y1=10,
                  line=dict(color="gold", dash="dash", width=1.5))


    # Etiqueta
    fig.add_annotation(
        x=3, y=4,
        text=f"Undesirable: {count_undesirable}",
        showarrow=False,
        font=dict(color="red", size=10, )
    )

    fig.add_annotation(
        x=7, y=9,
        text=f"Sub<br>optimal: {count_suboptimal}",
        showarrow=False,
        font=dict(color="orange", size=10)
    )

    # Ajustes finales
    fig.update_layout(
        template="plotly_white",
        xaxis=dict(range=[0, max_mht2 * 1.05]),
        yaxis=dict(range=[0, max_dbh2 * 1.05]),
        legend_title="Contract Info"
    )

    # Guardar como imagen PNG
    safe_name = f"{contract}_{farmer}".replace(" ", "_").replace("/", "_")
    fig.write_image(os.path.join(output_folder, f"{safe_name}.png"), width=1000, height=700, scale=3)
    print(os.path.join(output_folder, f"{safe_name}.png"))

print("✅ Todos los gráficos se han guardado exitosamente.")
