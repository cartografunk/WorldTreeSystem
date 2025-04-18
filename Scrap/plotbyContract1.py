import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
import os

# Conexión
engine = create_engine("postgresql://postgres:pauwlonia@localhost:5432/gisdb")

# Consulta SQL
query = """
SELECT inv."DBH (in)" AS dbh,
       inv."Merch. HT (ft)" AS mht,
       inv."ContractCode",
       f."FarmerName"
FROM public.inventory_us_2025 AS inv
JOIN public.cat_farmers AS f ON inv."ContractCode" = f."ContractCode"
JOIN public.cat_status AS s ON inv."Status"::bigint = s."id"
WHERE f."PlantingYear" = 2016
  AND s."AliveTree" = 1
  AND inv."DBH (in)" > 0
  AND inv."Merch. HT (ft)" > 0
"""

df = pd.read_sql(query, engine)

# Crear columna de leyenda personalizada
df["Contract Info"] = df["ContractCode"] + " – " + df["FarmerName"]

# Carpeta de salida
output_folder = r"D:\OneDrive\(0000) WorldTree\WorldTree\Graficas_por_contrato"
os.makedirs(output_folder, exist_ok=True)

# Constantes visuales
limite_dbh = 10
limite_mht = 6

# Obtener máximos globales
max_mht2 = df["mht"].max()
max_dbh2 = df["dbh"].max()


# Loop por contrato
for contract, group in df.groupby("ContractCode"):
    farmer = group["FarmerName"].iloc[0]
    contract_info = f"{contract} – {farmer}"
    group["Contract Info"] = contract_info

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

    # Zona no deseable 1: izquierda inferior
    fig.add_shape(type="rect", x0=0, x1=limite_mht, y0=0, y1=limite_dbh,
                  fillcolor="red", opacity=0.2, layer="below", line_width=0)

    # Zona no deseable 2: izquierda superior
    fig.add_shape(type="rect", x0=0, x1=limite_mht, y0=limite_dbh, y1=max_dbh2,
                  fillcolor="red", opacity=0.1, layer="below", line_width=0)

    # Zona no deseable 3: derecha inferior
    fig.add_shape(type="rect", x0=limite_mht, x1=max_mht2, y0=0, y1=limite_dbh,
                  fillcolor="red", opacity=0.1, layer="below", line_width=0)

    # Línea guía vertical
    fig.add_shape(type="line", x0=limite_mht, x1=limite_mht, y0=0, y1=max_dbh2,
                  line=dict(color="red", dash="dash", width=1))

    # Línea guía horizontal
    fig.add_shape(type="line", x0=0, x1=max_mht2, y0=limite_dbh, y1=limite_dbh,
                  line=dict(color="red", dash="dash", width=1))

    # Etiqueta
    fig.add_annotation(
        x=3, y=2,
        text="Undesirable trees",
        showarrow=False,
        font=dict(color="red", size=12)
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

print("✅ Todos los gráficos se han guardado exitosamente.")
