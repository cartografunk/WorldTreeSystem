# MonthlyReport/tables/t3_trees_by_planting_year.py
from core.libs import pd, np

DISPLAY_MAP  = {"CR": "Costa Rica", "GT": "Guatemala", "MX": "Mexico", "US": "USA"}
COUNTRIES    = ["Costa Rica", "Guatemala", "Mexico", "USA"]

def build_t3_trees_by_planting_year(
    mbt: pd.DataFrame,
    apply_filter: bool = False,                 # <- OFF por defecto (no filtra)
    filter_mode: str = "exclude_values",        # "isnull" | "exclude_values"
    filter_values: list[str] = None             # usado si filter_mode="exclude_values"
) -> pd.DataFrame:
    """
    T3:
    - Year = planting_year (fallback: year(planting_date) -> etp_year)
    - Contracted = trees_contract
    - Planted    = planted
    - Surviving  = current_surviving_trees (o alive_sc), capped con Planted
    - Filtro: por defecto NO se aplica. Activar con apply_filter=True.
        * filter_mode="isnull"        => usa solo filas con Filter IS NULL
        * filter_mode="exclude_values"=> excluye valores en filter_values (default ["Omit"])
    - Orden: Year asc y, dentro, Contracted→Planted→Surviving
    """
    if mbt is None or mbt.empty:
        return pd.DataFrame(columns=["Year","Status of Trees", *COUNTRIES, "Total","Survival %","Survival_Summary"])

    df = mbt.copy()

    # Country legible
    df["Country"] = df.get("region").replace(DISPLAY_MAP).fillna(df.get("region"))

    # Year preferente
    py = pd.to_numeric(df.get("planting_year"), errors="coerce").astype("Int64")
    if "planting_date" in df.columns:
        dt = pd.to_datetime(df["planting_date"], errors="coerce")
        py = py.fillna(dt.dt.year.astype("Int64"))
    py = py.fillna(pd.to_numeric(df.get("etp_year"), errors="coerce").astype("Int64"))
    df["planting_year"] = py

    # ==== Filtro opcional (activador) ====
    if apply_filter and "Filter" in df.columns:
        if filter_mode == "isnull":
            df = df[df["Filter"].isna()]
        else:  # "exclude_values"
            if filter_values is None:
                filter_values = ["Omit"]
            df = df[~df["Filter"].isin(filter_values)]

    # Tipado base
    for c in ["trees_contract", "planted", "current_surviving_trees", "alive_sc"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    # Métricas directas
    df["Contracted_use"] = pd.to_numeric(df.get("trees_contract"), errors="coerce").fillna(0)
    df["Planted_use"]    = pd.to_numeric(df.get("planted"),        errors="coerce").fillna(0)

    # Surviving (fallback y cap con Planted)
    surv_raw = pd.to_numeric(df.get("current_surviving_trees"), errors="coerce")
    if surv_raw.isna().all():
        surv_raw = pd.to_numeric(df.get("alive_sc"), errors="coerce")
    df["Surviving_use"] = surv_raw.fillna(0)
    df["Surviving_use"] = df[["Surviving_use", "Planted_use"]].min(axis=1).clip(lower=0)

    # ---------- Aggregación por planting_year x Country ----------
    def _make_pivot(colname: str) -> pd.DataFrame:
        tmp = (
            df.groupby(["planting_year", "Country"], dropna=False)[colname]
              .sum(min_count=1).reset_index()
              .pivot_table(index="planting_year", columns="Country", values=colname, aggfunc="sum", fill_value=0)
              .reset_index().rename(columns={"planting_year": "Year"})
        )
        for c in COUNTRIES:
            if c not in tmp.columns:
                tmp[c] = 0
        tmp["Total"] = tmp[COUNTRIES].sum(axis=1)
        return tmp[["Year", *COUNTRIES, "Total"]]

    piv_contracted = _make_pivot("Contracted_use")
    piv_planted    = _make_pivot("Planted_use")
    piv_surv       = _make_pivot("Surviving_use")

    # Survival % poblacional por Year = ΣS / ΣP
    rate = (
        piv_planted[["Year","Total"]].rename(columns={"Total":"P"})
        .merge(piv_surv[["Year","Total"]].rename(columns={"Total":"S"}), on="Year", how="outer")
        .fillna(0)
    )
    rate["Survival %"] = (rate["S"] / rate["P"]).where(rate["P"] > 0)

    # Summary (no ponderado) por contrato
    base_stats = df.copy()
    with np.errstate(divide="ignore", invalid="ignore"):
        base_stats["survival_pct"] = base_stats["Surviving_use"] / base_stats["Contracted_use"].replace(0, np.nan)
    summ = (
        base_stats.dropna(subset=["planting_year","survival_pct"])
        .groupby("planting_year")["survival_pct"]
        .apply(lambda s: None if s.empty else
               f"mean: {s.mean()*100:.1f}%, median: {s.median()*100:.1f}%, "
               f"max: {s.max()*100:.1f}%, min: {s.min()*100:.1f}%")
        .to_dict()
    )

    # ---------- Construcción de filas ----------
    def _rows_from_pivot(piv: pd.DataFrame, row_name: str) -> list[list]:
        r = []
        for _, rec in piv.sort_values("Year").iterrows():
            vals = [int(rec.get(c, 0)) for c in COUNTRIES]
            total = int(rec.get("Total", 0))
            surv_pct = ""
            surv_txt = ""
            if row_name == "Surviving":
                y = int(rec["Year"])
                s = rate.loc[rate["Year"] == y, "Survival %"]
                if not s.empty and pd.notna(s.iloc[0]):
                    surv_pct = f"{round(float(s.iloc[0]) * 100, 1)}%"
                surv_txt = summ.get(y, "") or ""
            r.append([int(rec["Year"]), row_name, *vals, total, surv_pct, surv_txt])
        return r

    rows = []
    rows += _rows_from_pivot(piv_contracted, "Contracted")
    rows += _rows_from_pivot(piv_planted,    "Planted")
    rows += _rows_from_pivot(piv_surv,       "Surviving")

    # Orden final
    body = pd.DataFrame(rows, columns=["Year", "Status of Trees", *COUNTRIES, "Total", "Survival %", "Survival_Summary"])
    body["Year"] = pd.to_numeric(body["Year"], errors="coerce")
    _status_rank = {"Contracted": 0, "Planted": 1, "Surviving": 2}
    body["_rank"] = body["Status of Trees"].map(_status_rank)
    body = body.sort_values(["Year", "_rank"], kind="mergesort").drop(columns="_rank").reset_index(drop=True)

    # Footer
    total_c = body.loc[body["Status of Trees"] == "Contracted", "Total"].sum()
    total_p = body.loc[body["Status of Trees"] == "Planted",    "Total"].sum()
    total_s = body.loc[body["Status of Trees"] == "Surviving",  "Total"].sum()
    footer = pd.DataFrame(
        [
            ["", "Total Contracted", *[""] * len(COUNTRIES), int(total_c), "", ""],
            ["", "Total Planted",    *[""] * len(COUNTRIES), int(total_p), "", ""],
            ["", "Total Surviving",  *[""] * len(COUNTRIES), int(total_s), "", ""],
        ],
        columns=["Year", "Status of Trees", *COUNTRIES, "Total", "Survival %", "Survival_Summary"],
    )

    return pd.concat([body, footer], ignore_index=True)
