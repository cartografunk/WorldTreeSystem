import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine

# Conexión a PostgreSQL
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

# Cargar datos
df = pd.read_sql(query, engine)

# Crear etiqueta personalizada para la leyenda
df["Contrato_Agricultor"] = df["ContractCode"] + " – " + df["FarmerName"]

# Gráfico actualizado
fig = px.scatter(
    df,
    x="mht",
    y="dbh",
    color="Contrato_Agricultor",
    hover_name="FarmerName",
    labels={"mht": "Merchantable Height (ft)", "dbh": "DBH (in)"},
    title="DBH vs MHT – Plantaciones 2016 (Árboles vivos por contrato)",
    opacity=0.7
)


# Agregar línea de corte en MHT = 6
fig.add_shape(type="line", x0=6, x1=6, y0=0, y1=df["dbh"].max(),
              line=dict(color="red", dash="dash"), name="MHT < 6")

# Agregar línea de corte en DBH = 10
fig.add_shape(type="line", x0=0, x1=df["mht"].max(), y0=10, y1=10,
              line=dict(color="red", dash="dash"), name="DBH < 10")

# Agregar rectángulo de área restringida
fig.add_shape(
    type="rect",
    x0=0, x1=6,
    y0=0, y1=10,
    fillcolor="grey",
    opacity=0.2,
    line_width=0,
    layer="below"
)

# Etiqueta dentro del área no deseable
fig.add_annotation(
    x=3, y=2,
    text="Non desirable trees",
    showarrow=False,
    font=dict(color="red", size=12)
)

# Layout final
fig.update_layout(template="plotly_white")
fig.show()
