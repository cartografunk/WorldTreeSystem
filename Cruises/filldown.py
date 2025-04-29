from utils.libs import pd
from utils.schema import COLUMNS

# Extraer HEADER_COLS desde schema.py
HEADER_COLS = [
    col["key"] for col in COLUMNS
    if col.get("source") in ("metadata", "input") and col["key"] in (
        "stand", "plot", "Permanent Plot", "short_note",
        "contractcode", "farmername", "cruisedate", "plot_coordinate", "Status"
    )
]

def forward_fill_headers(df: pd.DataFrame,
                         cols: list[str] | None = None) -> pd.DataFrame:
    """
    Aplica fill-down (ffill + bfill) a las columnas de encabezado.
    Devuelve una copia del DataFrame con valores heredados.
    """
    cols = cols or HEADER_COLS
    df_filled = df.copy()

    cols_existing = [col for col in cols if col in df_filled.columns]

    df_filled[cols_existing] = (
        df_filled[cols_existing]
        .replace("", pd.NA)
        .ffill()
        .bfill()
    )
    return df_filled
