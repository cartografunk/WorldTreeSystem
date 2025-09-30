# MonthlyReport/tables_process.py

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


def compute_allocation_type_contract(df: pd.DataFrame) -> pd.Series:
    def _num(col):
        if col not in df.columns:
            return pd.Series(0, index=df.index, dtype="float64")
        return pd.to_numeric(df[col], errors="coerce").fillna(0)

    year         = pd.to_numeric(df.get("etp_year"), errors="coerce").astype("Int64")
    usa_contract = _num("usa_trees_contracted")
    usa_planted  = _num("usa_trees_planted")
    can_contract = _num("canada_trees_contracted")
    can_total    = _num("total_can_allocation")
    can_2017     = _num("canada_2017_trees")
    cti_contract = _num("trees_contract")
    cti_planted  = _num("planted")
    usa_pct      = pd.to_numeric(df.get("usa_allocation_pct"), errors="coerce")

    # máscaras por cohorte
    is_cop_year = year.isin([2015, 2017])
    is_mix_year = year.isin([2016, 2018])
    is_etp_year = ~(is_cop_year | is_mix_year)

    out = pd.Series("", index=df.index, dtype="object")

    # COP puro
    out.loc[is_cop_year & ((can_contract + can_total + ((year == 2017).astype(int) * can_2017)) > 0)] = "COP"

    # COP/ETP — divide según usa_allocation_pct
    out.loc[is_mix_year & (usa_pct == 1)] = "ETP"
    out.loc[is_mix_year & (usa_pct == 0)] = "COP"
    out.loc[is_mix_year & (usa_pct > 0) & (usa_pct < 1)] = "COP/ETP"

    # ETP puro
    out.loc[is_etp_year] = "ETP"

    return out




def build_etp_trees(engine, etp_year, allocation_type):
    ca  = pd.read_sql("SELECT * FROM masterdatabase.contract_allocation", engine)
    cti = pd.read_sql("SELECT * FROM masterdatabase.contract_tree_information", engine)  # ← nombre correcto
    fpi = pd.read_sql("SELECT contract_code, region FROM masterdatabase.fpi_contracts_expanded", engine)
    fpi["region"] = normalize_region_series(fpi["region"])

    if allocation_type == "COP":
        # La región la aporta FPI
        df = (
            ca[ca["etp_year"] == etp_year]
            .merge(fpi, on="contract_code", how="left")
        )
        df_grouped = df.groupby("region", dropna=False)["total_can_allocation"].sum().reset_index()
        df_grouped.rename(columns={"total_can_allocation": "Total Trees"}, inplace=True)

    elif allocation_type == "ETP":
        # El número de árboles (planted) viene de CTI; la región, de FPI
        df = (
            cti[cti["etp_year"] == etp_year]
            .merge(fpi, on="contract_code", how="left")
        )
        df_grouped = df.groupby("region", dropna=False)["planted"].sum().reset_index()
        df_grouped.rename(columns={"planted": "Total Trees"}, inplace=True)

    elif allocation_type == "COP/ETP":
        # Sumar allocation CAN + planted USA (si aplica) y agrupar por región de FPI
        df = (
            ca[ca["etp_year"] == etp_year]
            .merge(fpi, on="contract_code", how="left")
        )
        df["combined_planted"] = df[["total_can_allocation", "usa_trees_planted"]].fillna(0).sum(axis=1)
        df_grouped = df.groupby("region", dropna=False)["combined_planted"].sum().reset_index()
        df_grouped.rename(columns={"combined_planted": "Total Trees"}, inplace=True)

    else:
        raise ValueError("allocation_type debe ser COP, ETP o COP/ETP")

    return df_grouped

    return df_grouped

def get_survival_data(engine):
    df = pd.read_sql("""
        SELECT
            cti.etp_year::int      AS etp_year,
            fpi.region             AS region,
            imc.survival           AS survival
        FROM masterdatabase.inventory_metrics_current imc
        JOIN masterdatabase.contract_tree_information       cti ON cti.contract_code = imc.contract_code
        JOIN masterdatabase.farmer_personal_information     fpi ON fpi.contract_code = imc.contract_code
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

def fmt_pct_1d(num, den):
    """Formato porcentaje con 1 decimal, o blank si den<=0 o NaN."""
    if pd.isna(num) or pd.isna(den) or den <= 0:
        return ""
    val = num / den
    return f"{round(val*100, 1)}%"

def _merge_back_geo_columns(df_enriched: pd.DataFrame, df_base: pd.DataFrame) -> pd.DataFrame:
    """
    Preserva/recupera columnas geo de t2 (Costa Rica, Guatemala, Mexico, USA) y Total
    después de enrich_with_obligations_and_stats, que a veces las pisa o las pierde.
    """
    GEO = ["Costa Rica", "Guatemala", "Mexico", "USA"]
    KEY = ["year", "etp", "contract_trees_status"]

    # 👇 Normaliza: si viene con etp_year pero no con year, crea year
    if "year" not in df_enriched.columns and "etp_year" in df_enriched.columns:
        df_enriched = df_enriched.copy()
        df_enriched["year"] = df_enriched["etp_year"]

    base_geo = df_base[KEY + [c for c in GEO + ["Total"] if c in df_base.columns]].copy()

    out = df_enriched.merge(base_geo, on=KEY, how="left", suffixes=("", "_base"))

    # Si la versión enriquecida no trae columnas geo o las trae en cero, usa las de *_base
    for c in GEO + ["Total"]:
        has_col = c in out.columns
        if not has_col and f"{c}_base" in out.columns:
            out[c] = out[f"{c}_base"]
        elif has_col and f"{c}_base" in out.columns:
            # Si quedó en 0 o NaN, rellena con base
            mask = out[c].isna() | (pd.to_numeric(out[c], errors="coerce").fillna(0) == 0)
            out.loc[mask, c] = out.loc[mask, f"{c}_base"]

        # Tipifica a int y limpia
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0).astype(int)
        if f"{c}_base" in out.columns:
            out.drop(columns=[f"{c}_base"], inplace=True)

    # Recalcula Total si sigue en 0 pero hay suma positiva en GEO
    if all(col in out.columns for col in GEO):
        s = out[GEO].sum(axis=1)
        mask_fix = (pd.to_numeric(out.get("Total", 0), errors="coerce").fillna(0) == 0) & (s > 0)
        out.loc[mask_fix, "Total"] = s.loc[mask_fix].astype(int)

    # Orden amigable si existen
    front = ["year", "etp", "contract_trees_status"]
    tail  = [c for c in ["Total", "Survival %", "Survival_Summary", "Series Obligation", "Obligation_Remaining"] if c in out.columns]
    mid   = [c for c in ["Costa Rica", "Guatemala", "Mexico", "USA"] if c in out.columns]
    cols  = [c for c in front + mid + tail if c in out.columns]
    return out[cols]


# MonthlyReport/tables_process.py

ALIAS_MAP = {
    "year": "ETP Year",
    "etp": "Type of ETP",
    "contract_trees_status": "Status of Trees",
    "Survival %": "Survival (%)",
    "Survival_Summary": "Survival by Contracts Summary",
    "Obligation_Remaining": "Obligation Remaining",
}

def apply_aliases(df):
    """Renombra columnas al formato final para Excel."""
    return df.rename(columns=ALIAS_MAP)
