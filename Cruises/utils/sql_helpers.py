# utils/sql_helpers.py
from sqlalchemy import Text, Float, Numeric, SmallInteger, Date
from utils.libs import pd
from utils.column_mapper import SQL_COLUMNS
from utils.schema import COLUMNS

# Construye FINAL_ORDER y DTYPES desde schema
SQL_COLUMNS = { col["key"]: col["sql_name"] for col in COLUMNS }
FINAL_ORDER = [ col["sql_name"] for col in COLUMNS ]
DTYPES = {
    col["sql_name"]: col["dtype"]
    for col in COLUMNS
    if "dtype" in col
}


def prepare_df_for_sql(df):
    # 1) renombrar internals → SQL
    df2 = df.rename(columns=SQL_COLUMNS)

    # 2) quitar duplicados
    df2 = df2.loc[:, ~df2.columns.duplicated()]

    # 3) filtrar+reordenar
    cols = [c for c in FINAL_ORDER if c in df2.columns]
    df2 = df2[cols].copy()

    # 4️⃣ Conversión de tipos en base a DTYPES
    for col, dtype in DTYPES.items():
        if col in df2.columns:
            if isinstance(dtype, SmallInteger):
                df2[col] = pd.to_numeric(df2[col], errors='coerce').fillna(0).astype(int)
            elif isinstance(dtype, (Float, Numeric)):
                df2[col] = pd.to_numeric(df2[col], errors='coerce')
            elif isinstance(dtype, Date):
                df2[col] = df2[col].where(df2[col].notna(), None)

    dtype_for_sql = {col: DTYPES[col] for col in df2.columns if col in DTYPES}

    return df2, dtype_for_sql