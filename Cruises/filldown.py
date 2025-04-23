#filldown.py
from utils.libs import pd

HEADER_COLS = [
    "stand", "plot", "permanent_plot_id", "short_note",
    "contractcode", "farmername", "cruisedate",
    "plot_coordinate", "status_id"
]

def forward_fill_headers(df: pd.DataFrame,
                         cols: list[str] | None = None) -> pd.DataFrame:
    """
    Aplica fill-down (ffill + bfill) a las columnas de encabezado.
    Devuelve una copia del DataFrame con valores heredados.
    """
    cols = cols or HEADER_COLS
    df_filled = df.copy()

    df_filled[cols] = (
        df_filled[cols]
        .replace("", pd.NA)       # cadenas vacías → NA
        .ffill()
        .bfill()
    )
    return df_filled
