from sqlalchemy import inspect
from core.db import get_engine
from core.schema import get_sqlalchemy_dtypes
import pandas as pd

engine = get_engine()
inspector = inspect(engine)
all_tables = inspector.get_table_names(schema='public')

# 1. Filtrar tablas tipo public.inventory_%
inventory_tables = [t for t in all_tables if t.startswith("inventory_")]

# 2. Obtener mapping de dtypes correcto desde schema.py
dtypes = get_sqlalchemy_dtypes()

# 3. Re-subir cada tabla con sus tipos correctos
for table in inventory_tables:
    print(f"📦 Reparchando: {table}")
    df = pd.read_sql(f"SELECT * FROM public.{table}", engine)

    # 👇 Limpieza opcional antes de volver a subir
    df = df.loc[:, ~df.columns.duplicated()].copy()

    from sqlalchemy.sql.sqltypes import Numeric, Float

    num_cols = [
        col for col, sqltype in dtypes.items()
        if isinstance(sqltype(), (Numeric, Float))
    ]

    for col in num_cols:
        if col in df.columns:
            df[col] = df[col].replace("", None)

    df.to_sql(
        name=table,
        con=engine,
        schema="public",
        if_exists="replace",  # cuidado si tienes constraints
        index=False,
        dtype=dtypes
    )

print("✅ Todos los inventarios han sido parchados con tipos correctos.")
