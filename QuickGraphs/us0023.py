from core.libs import pd
import plotly.express as px

from core.db import get_engine

engine = get_engine()
query = """
SELECT dbh_in, tht_ft, doyle_bf, contractcode
FROM inventory_us_2025
WHERE contractcode = 'US0023'
"""
df = pd.read_sql(query, engine)


# --- Scatterplot con Plotly ---
fig = px.scatter(
    df,
    x="tht_ft",
    y="dbh_in",
    hover_data=["doyle_bf"],
    labels={
        "doyle_bf": "Doyle BF"
    },
    title="US0023",
)
fig.update_traces(text=df['doyle_bf'], textposition='top center')
fig.show()

# Exportar a HTML interactivo
fig.write_html("scatterplot_us0023.html", auto_open=True)