#InventoryMetrics/generate_metrics.py
from core.libs import pd, create_engine, text
from core.db import get_engine
from InventoryMetrics.generate import create_cat_inventory_tables, get_inventory_tables

engine = get_engine()
tables = get_inventory_tables(engine)

create_cat_inventory_tables(engine, tables)

# Año y países a considerar
year = 2024
countries = ["gt", "cr", "mx", "us"]

# Construir consulta por país
queries = []
for country in countries:
    table = f"inventory_{country}_{year}"
    queries.append(f"""
    SELECT 
        '{country}' AS country_code,
        {year} AS year,
        contractcode,
        COUNT(*) AS total_trees,
        AVG(dbh) AS dbh_avg,
        STDDEV(dbh) AS dbh_sd,
        SUM(doyle_bf) AS doyle_total
    FROM {table}
    WHERE dbh IS NOT NULL AND doyle_bf IS NOT NULL
    GROUP BY contractcode
    """)

# Ejecutar todas juntas
sql = "\nUNION ALL\n".join(queries)
df = pd.read_sql(text(sql), engine)

# Insertar resultados en tabla central
df.to_sql("inventory_metrics", engine, schema="masterdatabase", if_exists="replace", index=False)
print("✅ Métricas insertadas en masterdatabase.inventory_metrics")
