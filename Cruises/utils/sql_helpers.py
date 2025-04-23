# utils/sql_helpers.py
from sqlalchemy import Text, Float, Numeric, SmallInteger, Date
from utils.libs import pd
from utils.schema import SQL_COLUMNS
from utils.schema import COLUMNS

# Construye FINAL_ORDER y DTYPES desde schema
FINAL_ORDER = [ entry["sql_name"] for entry in COLUMNS ]
DTYPES      = { entry["sql_name"]: entry["dtype"] for entry in COLUMNS }

def prepare_df_for_sql(df):
    # 1) renombrar internals → SQL
    df2 = df.rename(columns=SQL_COLUMNS)

    # 2) quitar duplicados
    df2 = df2.loc[:, ~df2.columns.duplicated()]

    # 3) filtrar+reordenar
    cols = [c for c in FINAL_ORDER if c in df2.columns]
    df2 = df2[[c for c in FINAL_ORDER if c in df2.columns]].copy()

    # 4️⃣ Conversión de tipos en base a DTYPES

    # Enteros pequeños
    int_cols = [c for c, dtype in DTYPES.items()
                if isinstance(dtype, SmallInteger) and c in df2.columns]
    for col, dtype in DTYPES.items():
        if col in df2.columns:
            if isinstance(dtype, (SmallInteger,)):
                df2[col] = pd.to_numeric(df2[col], errors="coerce").fillna(0).astype(int)
            elif isinstance(dtype, (Float, Numeric)):
                df2[col] = pd.to_numeric(df2[col], errors="coerce")

    return df2, dtype_for_sql