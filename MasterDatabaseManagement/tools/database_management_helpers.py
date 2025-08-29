#MasterdatabaseManagement/tools/database_management_helpers.py

from core.libs import pd

def remove_tz(df: pd.DataFrame) -> pd.DataFrame:
    # Quita tz de columnas datetime (robusto)
    for col in df.columns:
        if pd.api.types.is_datetime64tz_dtype(df[col]):
            df[col] = df[col].dt.tz_localize(None)
    return df
