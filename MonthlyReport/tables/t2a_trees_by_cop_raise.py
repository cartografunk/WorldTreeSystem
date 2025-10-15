# MonthlyReport/tables/t2a_cop_by_etp_raise.py
from core.libs import pd, np

COUNTRY_COLS = ["Costa Rica", "Guatemala", "Mexico", "USA"]
REGION_TO_COUNTRY = {"CR": "Costa Rica", "GT": "Guatemala", "MX": "Mexico", "US": "USA"}
FULFILLED_CUTOFF = 2023  # <= 2023 => "Fulfilled"

def build_cop_trees_table2(mbt: pd.DataFrame, so_by_year: dict | None = None) -> pd.DataFrame:
    """
    T2a (COP) desde MBT y un mapa series_obligation por año (so_by_year).
    - Filtros base: status != 'Out of Program', Filter != 'Omit'
    - Selección COP (decision tree):
        * 2015: siempre entra
        * 2017: entra si canada_2017_trees > 0
        * 2016 y 2018: entra si contracted_cop > 0 o planted_cop > 0
    - Métricas por país (COP): contracted_cop, planted_cop, surviving_cop
    - Survival by Contracts Summary (solo Filter IS NULL): surviving_cop / contracted_cop por contrato
    - Obligation Remaining (solo fila 'Surviving'):
        * y <= 2023 -> "Fulfilled"
        * y >= 2024 -> series_obligation(y) - Σ(contracted_cop del grupo)  (centralizado)
    """
    so_by_year = so_by_year or {}
    df = mbt.copy()

    # Country legible
    if "region" in df.columns:
        df["Country"] = df["region"].map(REGION_TO_COUNTRY).fillna(df.get("region"))

    # Filtros base (idénticos a T2)
    df = df[df["status"].fillna("").str.strip() != "Out of Program"]
    if "Filter" in df.columns:
        df = df[df["Filter"].fillna("") != "Omit"]

    # ---- Decision tree de inclusión COP ----
    y = pd.to_numeric(df.get("etp_year"), errors="coerce")
    contracted_cop = pd.to_numeric(df.get("contracted_cop", 0), errors="coerce").fillna(0)
    planted_cop    = pd.to_numeric(df.get("planted_cop", 0), errors="coerce").fillna(0)
    can2017        = pd.to_numeric(df.get("canada_2017_trees", 0), errors="coerce").fillna(0)

    mask_2015 = (y == 2015)
    mask_2017 = (y == 2017) & (can2017 > 0)
    mask_1618 = (y.isin([2016, 2018])) & ((contracted_cop > 0) | (planted_cop > 0))

    df = df[mask_2015 | mask_2017 | mask_1618].copy()
    df["Type of ETP"] = "COP"  # etiqueta fija para T2a

    # Métricas COP
    df["value__Contracted"] = contracted_cop
    df["value__Planted"]    = planted_cop
    df["value__Surviving"]  = pd.to_numeric(df.get("surviving_cop", 0), errors="coerce").fillna(0)

    rows = []
    for (etp_year, t), g in df.groupby(["etp_year", "Type of ETP"], dropna=True):
        for status, col in [
            ("Contracted", "value__Contracted"),
            ("Planted",    "value__Planted"),
            ("Surviving",  "value__Surviving"),
        ]:
            metric = g.groupby("Country", dropna=False)[col].sum(min_count=1)
            vals = {c: float(metric.get(c, 0.0) if c in metric.index else 0.0) for c in COUNTRY_COLS}
            total = float(sum(vals.values()))

            survival_summary = None
            obligation_remaining = None

            if status == "Surviving":
                # ---- Survival by Contracts Summary (solo Filter IS NULL) ----
                sub = g[g["Filter"].isna()] if "Filter" in g.columns else g
                den = pd.to_numeric(sub.get("contracted_cop", 0), errors="coerce").fillna(0)
                num = pd.to_numeric(sub.get("surviving_cop", 0), errors="coerce").fillna(0)
                with np.errstate(divide="ignore", invalid="ignore"):
                    ratio = (num / den.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan)
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

                # ---- Obligation Remaining (centralizado) ----
                contracted_total_cop = float(den.sum())
                so_val = so_by_year.get(int(etp_year)) if pd.notna(etp_year) else None
                if pd.notna(etp_year) and int(etp_year) <= FULFILLED_CUTOFF:
                    obligation_remaining = "Fulfilled"
                else:
                    if so_val is not None and not pd.isna(so_val):
                        obligation_remaining = float(so_val) - contracted_total_cop
                        if float(obligation_remaining).is_integer():
                            obligation_remaining = int(obligation_remaining)
                    else:
                        obligation_remaining = None

            rows.append({
                "ETP Year": int(etp_year) if pd.notna(etp_year) else None,
                "Type of ETP": t,  # 'COP'
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
