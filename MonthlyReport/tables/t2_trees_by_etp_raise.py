# MonthlyReport/tables/t2_trees_by_etp_raise.py
from core.libs import pd, np

COUNTRY_COLS = ["Costa Rica", "Guatemala", "Mexico", "USA"]
REGION_TO_COUNTRY = {"CR": "Costa Rica", "GT": "Guatemala", "MX": "Mexico", "US": "USA"}
FULFILLED_CUTOFF = 2023  # <= 2023 => "Fulfilled"

def _apply_filter(df, all_isnull: bool = True):
    """
    all_isnull=True  => conserva SOLO filas con Filter IS NULL (estricto)
    all_isnull=False => conserva filas excepto 'Omit' (modo previo)
    """
    if "Filter" not in df.columns:
        return df
    if all_isnull:
        return df[df["Filter"].isna()].copy()
    else:
        return df[df["Filter"].fillna("") != "Omit"].copy()


def build_etp_trees_table2(
    mbt: pd.DataFrame,
    so_by_year: dict | None = None,
    filter_isnull_all: bool = True,   # ← toggle rápido (True = solo Filter IS NULL)
) -> pd.DataFrame:
    """
    T2 (Trees by ETP) desde MBT + series_obligation opcional.
    Filtros:
      - Type of ETP in {'ETP','ETP/COP'}
      - Si filter_isnull_all=True: aplica Filter IS NULL a TODO (Contracted/Planted/Surviving)
        Si False: excluye solo 'Omit' (modo previo).
      - ⚠️ NO excluye 'Out of Program' (según lo acordado).
    """
    so_by_year = so_by_year or {}

    df = mbt.copy()

    # Country legible
    if "region" in df.columns:
        df["Country"] = df["region"].map(REGION_TO_COUNTRY).fillna(df.get("region"))

    # Filtra ETP / ETP/COP
    if "etp_type" in df.columns:
        df = df[df["etp_type"].isin(["ETP", "ETP/COP"])].copy()
        df = df.rename(columns={"etp_type": "Type of ETP"})
    else:
        df["Type of ETP"] = None

    # ✅ ÚNICO filtro por 'Filter'
    df = _apply_filter(df, all_isnull=filter_isnull_all)

    # Métricas
    df["value__Contracted"] = pd.to_numeric(df.get("contracted_etp", 0), errors="coerce").fillna(0)
    df["value__Planted"]    = pd.to_numeric(df.get("planted_etp",   0), errors="coerce").fillna(0)
    df["value__Surviving"]  = pd.to_numeric(df.get("current_surviving_trees", 0), errors="coerce").fillna(0)

    rows = []
    for (y, t), g in df.groupby(["etp_year", "Type of ETP"], dropna=True):
        for status, col in [
            ("Contracted", "value__Contracted"),
            ("Planted",    "value__Planted"),
            ("Surviving",  "value__Surviving"),
        ]:
            # ya viene filtrado globalmente (Filter IS NULL si aplica)
            metric = g.groupby("Country", dropna=False)[col].sum(min_count=1)
            vals = {c: float(metric.get(c, 0.0) if c in metric.index else 0.0) for c in COUNTRY_COLS}
            total = float(sum(vals.values()))

            survival_summary = None
            obligation_remaining = None

            if status == "Surviving":
                # Survival by Contracts Summary (usa el mismo g ya filtrado)
                contr = pd.to_numeric(g.get("trees_contract", 0), errors="coerce").fillna(0)
                surv  = pd.to_numeric(g.get("current_surviving_trees", 0), errors="coerce").fillna(0)
                with np.errstate(divide="ignore", invalid="ignore"):
                    ratio = (surv / contr).replace([np.inf, -np.inf], np.nan)
                s = ratio.dropna()
                if not s.empty:
                    def pct(v): return f"{v*100:.1f}%"
                    mean_v, med_v = s.mean(), s.median()
                    mode_v = ((s*100).round(1).value_counts().idxmax() / 100.0) if not s.empty else np.nan
                    max_v, min_v = s.max(), s.min()
                    rng_v = max_v - min_v
                    survival_summary = (
                        f"mean: {pct(mean_v)}, median: {pct(med_v)}, "
                        f"mode: {pct(mode_v) if pd.notna(mode_v) else 'NA'}, "
                        f"max: {pct(max_v)}, min: {pct(min_v)}, range: {pct(rng_v)}"
                    )

                # Obligation Remaining = series(y) - Σ(contracted_etp del grupo)
                contracted_total_etp = float(pd.to_numeric(g.get("contracted_etp", 0), errors="coerce").fillna(0).sum())
                so_val = so_by_year.get(int(y)) if pd.notna(y) else None
                if pd.notna(y) and int(y) <= FULFILLED_CUTOFF:
                    obligation_remaining = "Fulfilled"
                else:
                    if so_val is not None and not pd.isna(so_val):
                        obligation_remaining = float(so_val) - contracted_total_etp
                        if float(obligation_remaining).is_integer():
                            obligation_remaining = int(obligation_remaining)
                    else:
                        obligation_remaining = None

            rows.append({
                "ETP Year": int(y) if pd.notna(y) else None,
                "Type of ETP": t,
                "Status of Trees": status,
                **vals,
                "Total": total,
                "Survival by Contracts Summary": survival_summary,
                "Obligation Remaining": obligation_remaining,
            })

    out = pd.DataFrame(rows)
    if not out.empty:
        out = out[[
            "ETP Year", "Type of ETP", "Status of Trees",
            *COUNTRY_COLS, "Total",
            "Survival by Contracts Summary", "Obligation Remaining"
        ]].sort_values(["ETP Year", "Type of ETP", "Status of Trees"]).reset_index(drop=True)
    return out
