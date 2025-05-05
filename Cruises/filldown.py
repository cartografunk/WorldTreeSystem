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
    Aplica fill-down (ffill + bfill) solo a filas que no estén completamente vacías.
    Las filas vacías permanecen intactas.
    """
    cols = cols or HEADER_COLS
    df_filled = df.copy()

    # Asegurar que solo use columnas existentes
    existing = [col for col in cols if col in df_filled.columns]

    # Crear máscara de filas que tienen al menos un valor en esas columnas
    mask_non_empty = df_filled[existing].replace("", pd.NA).notna().any(axis=1)

    # Solo aplicar fill a esas filas
    df_filled.loc[mask_non_empty, existing] = (
        df_filled.loc[mask_non_empty, existing]
        .replace("", pd.NA)
        .ffill()
        .bfill()
    )

    return df_filled