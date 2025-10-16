# MonthlyReport/tables/t2a_cop_by_etp_raise.py
from core.libs import pd, np

COUNTRY_COLS = ["Costa Rica", "Guatemala", "Mexico", "USA"]
REGION_TO_COUNTRY = {"CR": "Costa Rica", "GT": "Guatemala", "MX": "Mexico", "US": "USA"}
FULFILLED_CUTOFF = 2023  # <= 2023 => "Fulfilled"

def build_cop_trees_table2(mbt: pd.DataFrame, so_by_year: dict | None = None) -> pd.DataFrame:
    from core.libs import pd, np

    REGION_TO_COUNTRY = {"CR": "Costa Rica", "GT": "Guatemala", "MX": "Mexico", "US": "USA"}
    COUNTRY_COLS = ["Costa Rica", "Guatemala", "Mexico", "USA"]
    FULFILLED_CUTOFF = 2023
    so_by_year = so_by_year or {}

    df = mbt.copy()
    df["Country"] = df.get("region").replace(REGION_TO_COUNTRY).fillna(df.get("region"))

    # Tipado
    y         = pd.to_numeric(df.get("etp_year"), errors="coerce")
    c_contr   = pd.to_numeric(df.get("contracted_cop", 0), errors="coerce").fillna(0)
    c_plant   = pd.to_numeric(df.get("planted_cop",    0), errors="coerce").fillna(0)
    c_surv    = pd.to_numeric(df.get("surviving_cop",  0), errors="coerce").fillna(0)
    can2017   = pd.to_numeric(df.get("canada_2017_trees", 0), errors="coerce").fillna(0)

    # 🔁 Año de agrupación: respeta el flag etp_2017
    etp2017_flag = df.get("etp_2017")
    if etp2017_flag is not None:
        is_2017_flag = etp2017_flag.fillna(False).astype(bool)
    else:
        # fallback: usa canada_2017_trees > 0 como proxy del flag
        is_2017_flag = (can2017 > 0)

    # group_year: si etp_2017 == True, fuerza 2017; si no, usa etp_year
    group_year = y.copy()
    group_year[is_2017_flag] = 2017
    df["group_year"] = group_year

    # Filtro base: NO quitamos OOP; solo excluimos 'Omit'
    base = pd.Series(True, index=df.index)
    if "Filter" in df.columns:
        base &= (df["Filter"].fillna("") != "Omit")

    # Reglas split COP (mismo criterio que T2, pero con el bucket 2017 por flag)
    has_cop  = (c_contr > 0) | (c_plant > 0)
    m2015    = (y == 2015) & has_cop
    m2017    = is_2017_flag & has_cop                   # ← usa el flag para 2017
    m1618    = y.isin([2016, 2018]) & has_cop

    keep = base & (m2015 | m2017 | m1618)
    dfk = df[keep].copy()
    if dfk.empty:
        return pd.DataFrame(columns=[
            "ETP Year","Type of ETP","Status of Trees",*COUNTRY_COLS,"Total",
            "Survival by Contracts Summary","Obligation Remaining"
        ])

    # Métricas COP (sin 'Planted')
    dfk["value__Contracted"] = c_contr.loc[dfk.index]
    dfk["value__Surviving"]  = c_surv.loc[dfk.index]
    dfk["Type of ETP"]       = "COP"

    rows = []
    for (grp_year, _), g in dfk.groupby(["group_year", "Type of ETP"], dropna=True):
        for status, col in [("Contracted","value__Contracted"),
                            ("Surviving","value__Surviving")]:  # 🚫 sin Planted
            metric = g.groupby("Country", dropna=False)[col].sum(min_count=1)
            vals = {c: float(metric.get(c, 0.0) if c in metric.index else 0.0) for c in COUNTRY_COLS}
            total = float(sum(vals.values()))

            survival_summary = None
            obligation_remaining = None

            if status == "Surviving":
                # Survival by Contracts Summary (solo Filter IS NULL)
                sub = g[g["Filter"].isna()] if "Filter" in g.columns else g
                den = pd.to_numeric(sub.get("contracted_cop", 0), errors="coerce").fillna(0)
                num = pd.to_numeric(sub.get("surviving_cop", 0), errors="coerce").fillna(0)
                with np.errstate(divide="ignore", invalid="ignore"):
                    s = (num / den.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).dropna()
                if not s.empty:
                    pct = lambda v: f"{v*100:.1f}%"
                    survival_summary = (
                        f"mean: {pct(s.mean())}, median: {pct(s.median())}, "
                        f"mode: {pct(((s*100).round(1).value_counts().idxmax()/100.0)) if not s.empty else 'NA'}, "
                        f"max: {pct(s.max())}, min: {pct(s.min())}, range: {pct(s.max()-s.min())}"
                    )

                # Obligation Remaining (COP)
                so_val = so_by_year.get(int(grp_year)) if pd.notna(grp_year) else None
                if pd.notna(grp_year) and int(grp_year) <= FULFILLED_CUTOFF:
                    obligation_remaining = "Fulfilled"
                else:
                    if so_val is not None and not pd.isna(so_val):
                        contracted_total_cop = float(den.sum())
                        obligation_remaining = float(so_val) - contracted_total_cop
                        if float(obligation_remaining).is_integer():
                            obligation_remaining = int(obligation_remaining)
                    else:
                        obligation_remaining = None

            rows.append({
                "ETP Year": int(grp_year) if pd.notna(grp_year) else None,  # ← usa el bucket
                "Type of ETP": "COP",
                "Status of Trees": status,
                **vals,
                "Total": total,
                "Survival by Contracts Summary": survival_summary,
                "Obligation Remaining": obligation_remaining,
            })

    out = pd.DataFrame(rows)
    if not out.empty:
        out["Status of Trees"] = pd.Categorical(out["Status of Trees"],
                                                categories=["Contracted","Surviving"],
                                                ordered=True)
        out = out[[
            "ETP Year","Type of ETP","Status of Trees",
            *COUNTRY_COLS,"Total",
            "Survival by Contracts Summary","Obligation Remaining"
        ]].sort_values(["ETP Year","Type of ETP","Status of Trees"]).reset_index(drop=True)
    return out
