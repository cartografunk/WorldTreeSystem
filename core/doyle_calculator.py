#core/doyle_calculator.py

from core.libs import pd
from core.schema_helpers import get_column

def calculate_doyle(df: pd.DataFrame, force_recalc=True) -> pd.DataFrame:
    """
    Calcula y añade/actualiza la columna 'doyle_bf' basada en DBH (in) y THT (ft).
    Usa los keys estándar (schema) y respeta aliases.
    """
    dbh_col = get_column("dbh_in", df)
    tht_col = get_column("tht_ft", df)
    doyle_col = get_column("doyle_bf", df) if "doyle_bf" in df.columns else "doyle_bf"

    df[dbh_col] = pd.to_numeric(df[dbh_col], errors='coerce')
    df[tht_col] = pd.to_numeric(df[tht_col], errors='coerce')

    if force_recalc or doyle_col not in df.columns:
        df[doyle_col] = ((df[dbh_col] - 4) ** 2) * (df[tht_col] / 16)
        print("🌳 Doyle recalculado")
    else:
        print("🌳 Doyle ya existe, no se recalcula (usa force_recalc=True para forzar)")

    return df
