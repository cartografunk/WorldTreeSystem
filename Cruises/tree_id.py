from core.libs import pd
from core.schema_helpers import get_column

PLOT_LOGICAL     = "Plot #"
TREE_LOGICAL     = "Tree #"
CONTRACT_LOGICAL = "ContractCode"

def pad_plot(val: str) -> str:
    val = str(val).strip()
    numeric = ''.join(filter(str.isdigit, val))
    alpha   = ''.join(filter(str.isalpha, val))
    return numeric.zfill(3) + alpha

def split_by_id_validity(df: pd.DataFrame):
    df = df.copy()

    plot_col     = get_column(PLOT_LOGICAL, df)
    tree_col     = get_column(TREE_LOGICAL, df)
    contract_col = get_column(CONTRACT_LOGICAL, df)

    df["plot_str"] = df[plot_col].astype(str).str.strip()
    df["plot_clean"] = df["plot_str"].apply(pad_plot)
    df["tree_num"] = pd.to_numeric(df[tree_col], errors="coerce")
    df["contractcode"] = df[contract_col]

    bad_mask = (
        df["contractcode"].isna() |
        df["contractcode"].str.strip().eq("") |
        df["plot_clean"].eq("") |
        df["tree_num"].isna()
    )
    ok = ~bad_mask

    df.loc[ok, "tree_padded"] = df.loc[ok, "tree_num"].astype(int).astype(str).str.zfill(3)
    df.loc[ok, "id"] = (
        df.loc[ok, "contractcode"].str.upper().str.strip() +
        df.loc[ok, "plot_clean"] +
        df.loc[ok, "tree_padded"]
    )

    def detect_id_error(row):
        if pd.isna(row["contractcode"]) or str(row["contractcode"]).strip() == "":
            return "❌ Falta contractcode"
        if pd.isna(row["plot_clean"]) or str(row["plot_clean"]).strip() == "":
            return "❌ Falta plot"
        if pd.isna(row["tree_num"]):
            return "❌ tree_number no numérico"
        return "❌ Desconocido"

    df.loc[bad_mask, "id_error"] = df.loc[bad_mask].apply(detect_id_error, axis=1)

    # Si tienes `path` en el df, inclúyelo
    if "path" in df.columns:
        df.loc[bad_mask, "id_error"] += df.loc[bad_mask]["path"].apply(lambda p: f" ← archivo: {os.path.basename(p)}")

    drop_cols = ["plot_str", "plot_clean", "tree_num", "tree_padded"]
    return (
        df.loc[ok].drop(columns=drop_cols),
        df.loc[bad_mask].drop(columns=drop_cols),
    )
