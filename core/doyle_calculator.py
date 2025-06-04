from core.libs import pd
from core.schema import get_column

def calculate_doyle(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula y añade la columna 'doyle_bf' basada en las columnas DBH (in) y THT (ft).
    """
    dbh_col = get_column("DBH (in)", df)
    tht_col = get_column("THT (ft)", df)

    df["DBH (in)"] = pd.to_numeric(df[dbh_col], errors='coerce')
    df["THT (ft)"] = pd.to_numeric(df[tht_col], errors='coerce')
    df["doyle_bf"] = ((df["DBH (in)"] - 4) ** 2) * (df["THT (ft)"] / 16)

    print("🌳 Doyle calculado")
    return df