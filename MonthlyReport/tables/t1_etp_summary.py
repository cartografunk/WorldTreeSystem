# MonthlyReport/tables/t1_etp_summary.py
from core.libs import pd, np
from MonthlyReport.tables_process import fmt_pct_1d

def _base_non_oop(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out = out[out["status"].fillna("").str.strip() != "Out of Program"]
    # Excluir SOLO "Omit" (no todo lo no-nulo)
    if "Filter" in out.columns:
        out = out[~(out["Filter"] == "Omit")]
    return out

def _agg_by_type(df: pd.DataFrame, alloc_type: str) -> pd.DataFrame:
    """
    Agrega por ('region','etp_year_raw') usando splits por tipo:
      - COP: surviving_cop / contracted_cop
      - ETP: surviving_etp / contracted_etp
    Además pivotea conteos por status y agrega (DEBUG) el array de contract_codes.
    """
    if alloc_type not in ("COP", "ETP"):
        raise ValueError("alloc_type debe ser 'COP' o 'ETP'.")

    # Filtro local T1: excluir SOLO 'Omit'
    if "Filter" in df.columns:
        df = df[~(df["Filter"] == "Omit")].copy()

    # Conteos por status (sobre el subconjunto ya filtrado por Omit)
    status_counts = (
        df.groupby(["region", "etp_year_raw", "status"], dropna=False)["contract_code"]
          .nunique()
          .unstack("status", fill_value=0)
          .reset_index()
    )

    # Total de contratos
    totals = (
        df.groupby(["region", "etp_year_raw"], dropna=False)["contract_code"]
          .nunique()
          .reset_index(name="Total Contracts")
    )

    # DEBUG: lista de códigos
    codes = (
        df.groupby(["region", "etp_year_raw"], dropna=False)["contract_code"]
          .apply(lambda s: sorted(pd.unique(s.dropna().astype(str))))
          .reset_index(name="Contract Codes")
    )

    # Survival sobre NON-OOP + sin Omit
    non_oop = _base_non_oop(df)
    if alloc_type == "COP":
        sums = non_oop.groupby(["region", "etp_year_raw"], dropna=False).agg(
            alive=("surviving_cop", "sum"),
            sampled=("contracted_cop", "sum"),
        ).reset_index()
    else:  # ETP
        sums = non_oop.groupby(["region", "etp_year_raw"], dropna=False).agg(
            alive=("surviving_etp", "sum"),
            sampled=("contracted_etp", "sum"),
        ).reset_index()

    out = status_counts.merge(totals, on=["region", "etp_year_raw"], how="left")
    out = out.merge(codes, on=["region", "etp_year_raw"], how="left")
    out = out.merge(sums, on=["region", "etp_year_raw"], how="left")

    # Survival (formateada)
    out["Survival"] = out.apply(
        lambda r: fmt_pct_1d(r.get("alive"), r.get("sampled"))
                  if pd.notna(r.get("sampled")) and r.get("sampled") > 0 else None,
        axis=1
    )

    # Etiquetas y orden
    out = out.rename(columns={"region": "Region", "etp_year_raw": "ETP Year"})
    out.insert(0, "Allocation Type", alloc_type)

    # Limpieza
    out = out.drop(columns=["alive", "sampled"], errors="ignore")
    return out

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

    fixed_left  = ["Allocation Type", "Region", "ETP Year", "Total Contracts", "Contract Codes"]
    fixed_right = ["Survival"]
    status_cols = [c for c in out.columns if c not in fixed_left + fixed_right]

    out = out.sort_values(by=["ETP Year", "Allocation Type", "Region"], na_position="last")
    out = out[fixed_left + status_cols + fixed_right].reset_index(drop=True)
    return out
