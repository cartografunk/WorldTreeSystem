#MonthlyReport/tables_process.py

from core.libs import pd

def weighted_mean(df, value_col, weight_col):
    valid = df[weight_col] > 0
    if valid.sum() == 0:
        return None
    return (df.loc[valid, value_col] * df.loc[valid, weight_col]).sum() / df.loc[valid, weight_col].sum()

def get_allocation_type(etp_year):
    if pd.isna(etp_year):
        return []

    if etp_year in [2015, 2017]:
        return ['COP']
    elif etp_year in [2016, 2018]:
        return ['COP', 'ETP']
    else:
        return ['ETP']


def build_etp_trees(engine, etp_year, allocation_type):
    ca = pd.read_sql("SELECT * FROM masterdatabase.contract_allocation", engine)
    cti = pd.read_sql("SELECT * FROM masterdatabase.contract_trees_info", engine)

    if allocation_type == "COP":
        df = ca[ca["etp_year"] == etp_year]
        df_grouped = df.groupby("region")["total_can_allocation"].sum().reset_index()
        df_grouped.rename(columns={"total_can_allocation": "Total Trees"}, inplace=True)

    elif allocation_type == "ETP":
        df = cti[cti["etp_year"] == etp_year]
        df_grouped = df.groupby("region")["planted"].sum().reset_index()
        df_grouped.rename(columns={"planted": "Total Trees"}, inplace=True)

    elif allocation_type == "COP/ETP":
        df = ca[ca["etp_year"] == etp_year]
        df["combined_planted"] = df[["total_can_allocation", "usa_trees_planted"]].fillna(0).sum(axis=1)
        df_grouped = df.groupby("region")["combined_planted"].sum().reset_index()
        df_grouped.rename(columns={"combined_planted": "Total Trees"}, inplace=True)

    else:
        raise ValueError("allocation_type debe ser COP, ETP o COP/ETP")

    return df_grouped

def get_survival_data(engine):
    df = pd.read_sql("""
        SELECT cti.etp_year, cti.region, imc.survival
        FROM masterdatabase.inventory_metrics_current imc
        JOIN masterdatabase.contract_tree_information cti
            ON imc.contract_code = cti.contract_code
        WHERE imc.survival IS NOT NULL
    """, engine)

    df["Survival"] = (df["survival"] * 100).round(1).astype(str) + "%"
    df["etp_year"] = df["etp_year"].astype("Int64")
    return df[["etp_year", "region", "Survival"]]

def _coerce_survival_pct(df: pd.DataFrame) -> pd.Series:
    """
    Devuelve una serie en [0,1] con survival pct, aceptando distintos nombres y formatos.
    Preferencias:
      1) Columna ya de survival (% o fracción)
      2) Derivarla de surviving/contracted o alive/total
    """
    df = df.copy()

    candidates = ["Survival", "Survival %", "survival", "survival_pct", "current_survival_pct"]
    for col in candidates:
        if col in df.columns:
            s = df[col]
            # Texto con "%"
            if s.dtype == object:
                s = pd.to_numeric(
                    s.astype(str).str.replace("%", "", regex=False).str.replace(",", ""),
                    errors="coerce"
                ) / 100.0
            else:
                s = pd.to_numeric(s, errors="coerce")
                # Si parece 0–100, pásalo a 0–1
                if s.max(skipna=True) is not None and s.max(skipna=True) > 1:
                    s = s / 100.0
            return s.clip(0, 1)

    # Derivar de contadores si existen
    if {"current_surviving_trees", "trees_contract"}.issubset(df.columns):
        num = pd.to_numeric(df["current_surviving_trees"], errors="coerce")
        den = pd.to_numeric(df["trees_contract"], errors="coerce")
        return (num / den).replace([pd.NA, pd.NaT], pd.NA).clip(0, 1)

    if {"Alive", "Total Trees"}.issubset(df.columns):
        num = pd.to_numeric(df["Alive"], errors="coerce")
        den = pd.to_numeric(df["Total Trees"], errors="coerce")
        return (num / den).replace([pd.NA, pd.NaT], pd.NA).clip(0, 1)

    raise KeyError(
        "No encontré columnas para calcular Survival ( intenta proveer 'current_survival_pct' "
        "o 'current_surviving_trees' y 'trees_contract')."
    )

def format_survival_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Recibe el DF original (como llega a enrich_with_obligations_and_stats) y agrega columnas normalizadas
    'Survival_pct' (0–1) y 'Survival' (texto con %), sin depender de un nombre fijo de origen.
    """
    df = df.copy()
    surv = _coerce_survival_pct(df)
    df["Survival_pct"] = surv
    df["Survival"] = (surv * 100).round(2).astype("Float64").astype(str) + "%"

    # Si aquí haces más agregaciones/resúmenes, continúa igual…
    # Ejemplo (ajústalo a tu lógica actual):
    # resumen = df.groupby("etp_year", dropna=False)["Survival_pct"].mean().reset_index()
    # resumen["Survival"] = (resumen["Survival_pct"] * 100).round(2).astype(str) + "%"
    # return resumen

    return df


def clean_t2a_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia la tabla T2A antes de exportar:
    - Elimina columnas de stats numéricas que no se exportan (mean, median, mode, max, min, range)
    - Reordena columnas para que Survival_Summary quede antes de Obligation_Remaining (si existen)
    """
    scrap_cols = ["mean", "median", "mode", "max", "min", "range"]
    df_clean = df.drop(columns=[c for c in scrap_cols if c in df.columns], errors="ignore")

    if "Survival_Summary" in df_clean.columns and "Obligation_Remaining" in df_clean.columns:
        cols = list(df_clean.columns)
        cols.remove("Survival_Summary")
        idx = cols.index("Obligation_Remaining")
        cols.insert(idx, "Survival_Summary")
        df_clean = df_clean[cols]

    return df_clean