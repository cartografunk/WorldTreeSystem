from core.libs import pd
from utils.schema import COLUMNS
from sqlalchemy import Text, Float, SmallInteger

# Filtro solo los campos de auditoría definidos en schema
AUDIT_KEYS = {
    "contractcode",
    "farmername",
    "plantingyear",
    "trees_sampled",
    "contracted_trees",
    "sample_size",
    "total_deads",
    "mortality",
    "total_alive",
    "survival",
    "remaining_trees",
}

AUDIT_COLUMNS = [col for col in COLUMNS if col["key"] in AUDIT_KEYS]

AUDIT_ORDER = [col["sql_name"] for col in AUDIT_COLUMNS]
AUDIT_DTYPES = {
    col["sql_name"]: col.get("dtype", Text()) for col in AUDIT_COLUMNS
}

SQL_COLUMNS = {
    col["key"]: col["sql_name"] for col in AUDIT_COLUMNS
}


def prepare_audit_for_sql(df):
    df2 = df.rename(columns=SQL_COLUMNS)
    df2 = df2.loc[:, ~df2.columns.duplicated()]
    cols = [c for c in AUDIT_ORDER if c in df2.columns]
    df2 = df2[cols].copy()

    for col, dtype in AUDIT_DTYPES.items():
        if col in df2.columns:
            if isinstance(dtype, SmallInteger):
                df2[col] = pd.to_numeric(df2[col], errors='coerce').fillna(0).astype("Int64")
            elif isinstance(dtype, Float):
                df2[col] = pd.to_numeric(df2[col], errors='coerce')
            else:
                df2[col] = df2[col].astype(str)

    dtype_for_sql = {col: AUDIT_DTYPES[col] for col in df2.columns if col in AUDIT_DTYPES}
    return df2, dtype_for_sql
