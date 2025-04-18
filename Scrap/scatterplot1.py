import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine

# --- Conexión a PostgreSQL ---
engine = create_engine("postgresql://postgres:pauwlonia@localhost:5432/gisdb")

# --- Consulta SQL corregida y final ---
query = """
SELECT inv."DBH (in)" AS dbh,
       inv."Merch. HT (ft)" AS mht
FROM public.inventory_us_2025 AS inv
JOIN public.cat_farmers AS f ON inv."ContractCode" = f."ContractCode"
JOIN public.cat_status AS s ON inv."Status"::bigint = s."id"
WHERE f."PlantingYear" = 2016
  AND s."AliveTree" = 1
  AND inv."DBH (in)" > 0
  AND inv."Merch. HT (ft)" > 0
"""

# --- Leer datos y graficar ---
df = pd.read_sql(query, engine)

fig = px.scatter(
    df,
    x="mht",  # Ahora MHT en X
    y="dbh",  # DBH en Y
    labels={"mht": "Merchantable Height (ft)", "dbh": "DBH (in)"},
    title="DBH vs MHT – Plantaciones 2016 (Árboles vivos)",
    opacity=0.7
)


fig.update_layout(template="plotly_white")
fig.show()
