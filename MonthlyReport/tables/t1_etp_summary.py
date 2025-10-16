# MonthlyReport/tables/t1_etp_summary.py
from core.libs import pd, np
from MonthlyReport.tables_process import fmt_pct_1d

def _subset_for_survival(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out = out[out["status"].fillna("").str.strip() != "Out of Program"]
    if "Filter" in out.columns:
        f = out["Filter"]  # <-- NO castear aquí
        keep = f.isna() | (f.astype(str).str.strip() == "")
        out = out[keep]
    return out

def _agg_by_type(df: pd.DataFrame, alloc_type: str) -> pd.DataFrame:
    """
    Agrega por ('region','etp_year_raw') usando splits por tipo:
      - Contracted / Planted: SIN filtrar Out of Program (sí respeta Filter != 'Omit')
      - Surviving y Survival: EXCLUYENDO Out of Program
    También pivotea conteos por status y agrega (DEBUG) el array de contract_codes.
    """
    if alloc_type not in ("COP", "ETP"):
        raise ValueError("alloc_type debe ser 'COP' o 'ETP'.")

    # ---------- 1) Subconjunto para métricas SIN filtrar OOP (solo quita Omit) ----------
    df_no_omit = df.copy()
    if "Filter" in df_no_omit.columns:
        df_no_omit = df_no_omit[~(df_no_omit["Filter"] == "Omit")].copy()

    # ---------- 2) Conteos por status (sobre df_no_omit, es decir SIN Omit, CON OOP) ----------
    status_counts = (
        df_no_omit.groupby(["region", "etp_year_raw", "status"], dropna=False)["contract_code"]
          .nunique()
          .unstack("status", fill_value=0)
          .reset_index()
    )

    # Total de contratos (SIN Omit, CON OOP)
    totals = (
        df_no_omit.groupby(["region", "etp_year_raw"], dropna=False)["contract_code"]
          .nunique()
          .reset_index(name="Total Contracts")
    )

    # DEBUG: lista de códigos (SIN Omit, CON OOP)
    codes = (
        df_no_omit.groupby(["region", "etp_year_raw"], dropna=False)["contract_code"]
          .apply(lambda s: sorted(pd.unique(s.dropna().astype(str))))
          .reset_index(name="Contract Codes")
    )

    # ---------- 3) Métricas de Contracted / Planted (SIN OOP) ----------
    # Columnas preferentes por tipo; caen a nombres genéricos si no existen
    contracted_col = f"contracted_{alloc_type.lower()}" if f"contracted_{alloc_type.lower()}" in df_no_omit.columns else (
        "trees_contract" if "trees_contract" in df_no_omit.columns else None
    )
    planted_col = f"planted_{alloc_type.lower()}" if f"planted_{alloc_type.lower()}" in df_no_omit.columns else (
        "planted" if "planted" in df_no_omit.columns else None
    )

    sums_totals_parts = {}
    if contracted_col is not None:
        sums_totals_parts["Contracted"] = (contracted_col, "sum")
    if planted_col is not None:
        sums_totals_parts["Planted"] = (planted_col, "sum")

    sums_totals = (
        df_no_omit.groupby(["region", "etp_year_raw"], dropna=False)
        .agg(**sums_totals_parts)
        .reset_index()
        if sums_totals_parts else
        df_no_omit.groupby(["region", "etp_year_raw"], dropna=False).size().reset_index(name="__dummy__").drop(columns="__dummy__")
    )

    # ---------- 4) Métricas de Surviving/Survival (EXCLUYENDO OOP) ----------
    df_active = _subset_for_survival(df)  # quita OOP y cualquier Filter no-nulo
    surviving_col = f"surviving_{alloc_type.lower()}" if f"surviving_{alloc_type.lower()}" in df_active.columns else (
        "surviving_current" if "surviving_current" in df_active.columns else None
    )
    contracted_active_col = f"contracted_{alloc_type.lower()}" if f"contracted_{alloc_type.lower()}" in df_active.columns else (
        "trees_contract" if "trees_contract" in df_active.columns else None
    )

    sums_survival_parts = {}
    if surviving_col is not None:
        sums_survival_parts["Surviving"] = (surviving_col, "sum")
    if contracted_active_col is not None:
        sums_survival_parts["__ContractedActive__"] = (contracted_active_col, "sum")

    sums_survival = (
        df_active.groupby(["region", "etp_year_raw"], dropna=False)
        .agg(**sums_survival_parts)
        .reset_index()
        if sums_survival_parts else
        df_active.groupby(["region", "etp_year_raw"], dropna=False).size().reset_index(name="__dummy__").drop(columns="__dummy__")
    )

    # ---------- 5) Merge y Survival ----------
    out = status_counts.merge(totals, on=["region", "etp_year_raw"], how="left")
    out = out.merge(codes, on=["region", "etp_year_raw"], how="left")
    out = out.merge(sums_totals, on=["region", "etp_year_raw"], how="left")  # trae Planted (sin OOP)
    out = out.merge(sums_survival, on=["region", "etp_year_raw"], how="left")  # trae Surviving (sin OOP)

    # % Survival = Surviving (activo) / Planted (universo sin Omit)
    planted_col_name = "Planted" if "Planted" in out.columns else None
    if {"Surviving"}.issubset(out.columns) and planted_col_name:
        out["Survival"] = out.apply(
            lambda r: fmt_pct_1d(r["Surviving"], r[planted_col_name])
            if pd.notna(r[planted_col_name]) and r[planted_col_name] > 0 else None,
            axis=1
        )
    else:
        out["Survival"] = None

    # Limpieza y etiquetas
    out = out.rename(columns={"region": "Region", "etp_year_raw": "ETP Year"})
    out.insert(0, "Allocation Type", alloc_type)
    return out


# _select_year_type_subset(...) y build_etp_summary(...) quedan igual


def _select_year_type_subset(mbt: pd.DataFrame, year: int, alloc_type: str) -> pd.DataFrame:
    base = mbt.copy()

    if alloc_type == "COP":
        if year == 2017:
            # 2017 COP: contratos marcados etp_2017=True
            subset = base[(base.get("etp_2017", False) == True) & (base.get("contracted_cop", 0) > 0)].copy()
            subset["etp_year_raw"] = 2017  # agrupar bajo 2017
            return subset
        else:
            # No excluimos etp_2017 aquí: contamos normalmente por año
            return base[(base["etp_year_raw"] == year) & (base.get("contracted_cop", 0) > 0)].copy()
    else:  # ETP
        if year == 2017:
            # 2017 no tiene ETP
            return base.iloc[0:0].copy()
        else:
            return base[(base["etp_year_raw"] == year) & (base.get("contracted_etp", 0) > 0)].copy()

def build_etp_summary(mbt: pd.DataFrame) -> pd.DataFrame:
    """
    Construye T1 ETP Summary desde MBT (NO consulta BD).
    Requiere que MBT traiga:
      - region, status, Filter
      - etp_year_raw, etp_2017
      - contracted_cop/etp, surviving_cop/etp
    """
    # Años presentes en MBT
    years = sorted(pd.to_numeric(mbt["etp_year_raw"], errors="coerce").dropna().astype(int).unique().tolist())

    frames = []
    for y in years:
        # COP
        cop_df = _select_year_type_subset(mbt, y, "COP")
        if not cop_df.empty:
            frames.append(_agg_by_type(cop_df, "COP"))
        # ETP
        etp_df = _select_year_type_subset(mbt, y, "ETP")
        if not etp_df.empty:
            frames.append(_agg_by_type(etp_df, "ETP"))

    if not frames:
        return pd.DataFrame(columns=["Allocation Type", "Region", "ETP Year", "Total Contracts", "Contract Codes", "Survival"])

    out = pd.concat(frames, ignore_index=True)

    # Orden y formato final
    cat_order = pd.CategoricalDtype(categories=["COP", "ETP"], ordered=True)
    out["Allocation Type"] = out["Allocation Type"].astype(cat_order)
    out["ETP Year"] = pd.to_numeric(out["ETP Year"], errors="coerce").astype("Int64").astype("string")
    out.loc[out["ETP Year"].isin(["<NA>", "nan"]), "ETP Year"] = "Not asigned yet"

    # Limpieza simple y alias en este punto
    out = (
        out
        .drop(columns=["Contracted", "Planted", "Surviving", "__ContractedActive__"], errors="ignore")
        .rename(columns={"Survival": "Survival %"})
    )

    # Quitar del resultado final la columna de depuración, si existe
    out = out.drop(columns=["Contract Codes"], errors="ignore")

    # Columnas fijas (solo las que existan en el DF, conservando orden)
    fixed_left_base = ["Allocation Type", "Region", "ETP Year", "Total Contracts"]
    fixed_left = [c for c in fixed_left_base if c in out.columns]

    fixed_right = [c for c in ["Survival %"] if c in out.columns]

    # El resto son columnas de status (en el orden actual del DF)
    status_cols = [c for c in out.columns if c not in set(fixed_left) | set(fixed_right)]

    # Orden de filas (solo aplica claves que existan)
    sort_keys = [c for c in ["ETP Year", "Allocation Type", "Region"] if c in out.columns]
    if sort_keys:
        out = out.sort_values(by=sort_keys, na_position="last")

    # Reordenar columnas y devolver
    out = out[fixed_left + status_cols + fixed_right].reset_index(drop=True)
    return out


