#Cruises/filldown
from core.libs import pd
from core.schema import COLUMNS

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
    Las filas vacías en columnas clave se eliminan antes.
    """
    cols = cols or HEADER_COLS
    df_filled = df.copy()

    # Asegurar que solo use columnas existentes
    existing = [col for col in cols if col in df_filled.columns]

    # 🔥 Eliminar filas completamente vacías en columnas clave
    df_filled = df_filled.replace("", pd.NA)
    df_filled = df_filled.dropna(subset=existing, how="all")

    # ✅ Aplicar filldown solo a las filas parcialmente completas
    mask_non_empty = df_filled[existing].notna().any(axis=1)
    df_filled.loc[mask_non_empty, existing] = (
        df_filled.loc[mask_non_empty, existing]
        .ffill()
        .bfill()
    )

    return df_filled
