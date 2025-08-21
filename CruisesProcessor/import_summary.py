#CruisesProcessor/import_summary
from core.libs import pd, Path

def generate_summary_from_df(df: pd.DataFrame, archivos: list[str]) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    resumen = (
        df.groupby("contractcode")
          .agg(total_filas=("tree_number", "count"))
          .reset_index()
    )

    # Buscar archivo que contenga el contractcode (mejor forma: basename match)
    def match_archivo(contract):
        for f in archivos:
            name = Path(f).stem
            if contract in name:
                return f
        return "—"

    resumen["archivo"] = resumen["contractcode"].apply(match_archivo)

    return resumen[["contractcode", "archivo", "total_filas"]]
