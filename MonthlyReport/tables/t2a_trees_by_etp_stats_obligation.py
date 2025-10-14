# MonthlyReport/tables/t2a_trees_by_etp_stats_obligation.py

from core.libs import pd, np
from MonthlyReport.tables_process import clean_t2a_for_excel, get_allocation_type

def _infer_years(df_in: pd.DataFrame) -> set[int]:
    if not isinstance(df_in, pd.DataFrame):
        return set()
    for col in ("etp_year", "ETP Year", "year"):
        if col in df_in.columns:
            vals = pd.to_numeric(df_in[col], errors="coerce").dropna().astype(int).unique().tolist()
            if vals:
                return set(int(v) for v in vals)
    return set()

def _infer_etp_label(df_in: pd.DataFrame, years: set[int]) -> str:
    # si viene 'etp' y es consistente, úsalo
    if isinstance(df_in, pd.DataFrame) and "etp" in df_in.columns:
        vals = [str(v) for v in df_in["etp"].dropna().unique().tolist()]
        if len(vals) == 1:
            return vals[0]
    # si no, inferimos por años (regla global)
    has_etp = any("ETP" in get_allocation_type(y) for y in years) if years else True
    has_cop = any("COP" in get_allocation_type(y) for y in years) if years else False
    if has_cop and not has_etp:
        return "COP"
    if has_etp and not has_cop:
        return "ETP"
    return "ETP/COP"

def _pct(x):
    return f"{x*100:.1f}%" if pd.notna(x) else "NA"

def enrich_with_obligations_and_stats(df_in: pd.DataFrame, mbt: pd.DataFrame) -> pd.DataFrame:
    """
    T2A desde MBT (sin DB).
    - OUT: status='Out of Program' y Filter='Omit'
    - Stats de survival sólo con Filter IS NULL
    - Planted: 'planted_cop' si etp ∈ {COP, ETP/COP}; si no, 'planted_etp'
    - Obligation_Remaining = Contracted - Planted  (si quieres usar series_obligation más adelante, agrégala a MBT)
    """
    years = _infer_years(df_in)
    etp_label = _infer_etp_label(df_in, years)

    df = mbt.copy()

    # Filtros estándar y por años
    df = df[df["status"].fillna("").str.strip() != "Out of Program"]
    if "Filter" in df.columns:
        df = df[df["Filter"].fillna("") != "Omit"]
    if years:
        df = df[df["etp_year"].isin(years)]

    # Numéricos seguros según tus headers de MBT
    for col in ("trees_contract", "current_surviving_trees", "planted_etp", "planted_cop"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        else:
            df[col] = 0.0

    planted_col = "planted_cop" if etp_label in {"COP", "ETP/COP"} else "planted_etp"

    # Agregados por año
    agg = (
        df.groupby("etp_year", dropna=True)
          .agg(
              Contracted=("trees_contract", "sum"),
              Planted=(planted_col, "sum"),
              Surviving=("current_surviving_trees", "sum"),
          )
          .reset_index()
    )

    # Survival stats por contrato (solo Filter IS NULL)
    stats_base = df[df["Filter"].isna()] if "Filter" in df.columns else df
    contr = pd.to_numeric(stats_base.get("trees_contract", 0), errors="coerce").fillna(0)
    surv  = pd.to_numeric(stats_base.get("current_surviving_trees", 0), errors="coerce").fillna(0)
    with np.errstate(divide="ignore", invalid="ignore"):
        ratio = (surv / contr).replace([np.inf, -np.inf], np.nan)

    def _mode_pct(s):
        s = pd.to_numeric(s, errors="coerce").dropna()
        if s.empty: return np.nan
        s_pct = (s * 100).round(1)
        vc = s_pct.value_counts()
        return vc.index[0] / 100.0 if not vc.empty else np.nan

    stats_num = (
        stats_base.assign(_r=ratio)
        .groupby("etp_year")["_r"]
        .agg(mean="mean", median="median", mode=_mode_pct, max="max", min="min")
        .reset_index()
    )
    stats_num["range"] = stats_num["max"] - stats_num["min"]
    stats_num["Survival by Contracts Summary"] = (
        "mean: " + stats_num["mean"].map(_pct)
        + ", median: " + stats_num["median"].map(_pct)
        + ", mode: " + stats_num["mode"].map(_pct)
        + ", max: " + stats_num["max"].map(_pct)
        + ", min: " + stats_num["min"].map(_pct)
        + ", range: " + stats_num["range"].map(_pct)
    )

    # Unimos stats a la fila Surviving y calculamos Obligation_Remaining
    t2a_surv = agg.merge(stats_num[["etp_year", "Survival by Contracts Summary"]], on="etp_year", how="left")
    t2a_surv["Obligation_Remaining"] = (
        pd.to_numeric(t2a_surv["Contracted"], errors="coerce").fillna(0)
        - pd.to_numeric(t2a_surv["Planted"], errors="coerce").fillna(0)
    ).clip(lower=0)

    # Expandir a 3 filas por año
    recs = []
    for _, r in agg.iterrows():
        y = int(r["etp_year"])
        recs += [
            {"etp_year": y, "contract_trees_status": "Contracted", "Total": float(r["Contracted"])},
            {"etp_year": y, "contract_trees_status": "Planted",    "Total": float(r["Planted"])},
            {"etp_year": y, "contract_trees_status": "Surviving",  "Total": float(r["Surviving"])},
        ]
    base_long = pd.DataFrame.from_records(recs)

    surv_rows = base_long[base_long["contract_trees_status"] == "Surviving"].merge(
        t2a_surv, on="etp_year", how="left"
    )

    out = pd.concat(
        [base_long[base_long["contract_trees_status"] != "Surviving"], surv_rows],
        ignore_index=True
    )
    out["etp"] = etp_label

    # Limpieza y orden final
    out = clean_t2a_for_excel(out)
    order = ["Contracted", "Planted", "Surviving"]
    out["contract_trees_status"] = pd.Categorical(out["contract_trees_status"], order, True)
    out = out.sort_values(["etp_year", "etp", "contract_trees_status"]).reset_index(drop=True)
    return out
