import pandas as pd
from sqlalchemy import create_engine, text

# — Configuración —
conn_string = "postgresql+psycopg2://postgres:pauwlonia@localhost:5432/gisdb"
table_name = "US_InventoryDatabase_Q1_2025b"
engine = create_engine(conn_string)

# — 1) Cargar datos —
cols_needed = ["ContractCode", "Plot #", "Tree #", "Plot Coordinate"]
df = pd.read_sql(f'SELECT {", ".join(f"\"{c}\"" for c in cols_needed)} FROM "{table_name}"', engine)

# — 2) Identificar plots inconsistentes —
inconsistentes = (
    df.groupby(["ContractCode", "Plot #"])["Plot Coordinate"]
    .filter(lambda x: x.nunique() > 1)
)

if not inconsistentes.empty:
    # — 3) Calcular correcciones (primer valor no nulo por Tree #) —
    correcciones = (
        df.sort_values("Tree #")
        .groupby(["ContractCode", "Plot #"])["Plot Coordinate"]
        .first()
        .reset_index()
    )

    # — 4) Aplicar actualizaciones —
    with engine.begin() as conn:
        for _, row in correcciones.iterrows():
            conn.execute(
                text(f"""
                UPDATE "{table_name}"
                SET "Plot Coordinate" = :coord
                WHERE "ContractCode" = :contract
                AND "Plot #" = :plot
                AND ("Plot Coordinate" IS DISTINCT FROM :coord)
                """),
                {"coord": row["Plot Coordinate"],
                 "contract": row["ContractCode"],
                 "plot": row["Plot #"]}
            )

        print(f"✅ Actualizados {len(correcciones)} plots inconsistentes")
else:
    print("✅ Todos los plots ya tienen coordenadas consistentes (como muestras en tus datos)")