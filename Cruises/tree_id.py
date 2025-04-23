# tree_id.py
from utils.libs import pd
from utils.cleaners import get_column

PLOT_LOGICAL     = "Plot #"
TREE_LOGICAL     = "Tree #"
CONTRACT_LOGICAL = "ContractCode"      # coincide con COLUMN_LOOKUP

def split_by_id_validity(df: pd.DataFrame):
    """
    Devuelve df_good y df_bad usando columnas resueltas con get_column.
    """
    df = df.copy()

    # ── resolver nombres de columnas ───────────────────────
    plot_col     = get_column(df, PLOT_LOGICAL)       # e.g. "plot"
    tree_col     = get_column(df, TREE_LOGICAL)       # e.g. "tree_number"
    contract_col = get_column(df, CONTRACT_LOGICAL)   # e.g. "contractcode"

    # ── convertir a numérico y validar ────────────────────
    df["plot_num"] = pd.to_numeric(df[plot_col], errors="coerce")
    df["tree_num"] = pd.to_numeric(df[tree_col], errors="coerce")
    df["contractcode"] = df[contract_col]

    bad_mask = (
        df["contractcode"].isna() |
        df["contractcode"].str.strip().eq("") |
        df["plot_num"].isna() |
        df["tree_num"].isna()
    )
    ok = ~bad_mask

    df.loc[ok, "plot_padded"] = df.loc[ok, "plot_num"].astype(int).astype(str).str.zfill(2)
    df.loc[ok, "tree_padded"] = df.loc[ok, "tree_num"].astype(int).astype(str).str.zfill(3)
    df.loc[ok, "id"] = (
        df.loc[ok, "contractcode"].str.upper().str.strip()
        + df.loc[ok, "plot_padded"]
        + df.loc[ok, "tree_padded"]
    )

    df.loc[bad_mask, "id_error"] = "Contrato/Plot/Tree vacío o no numérico"

    drop_cols = ["plot_num", "tree_num", "plot_padded", "tree_padded"]
    return (
        df.loc[ok].drop(columns=drop_cols),
        df.loc[bad_mask].drop(columns=drop_cols),
    )
