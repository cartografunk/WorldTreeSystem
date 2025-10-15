# MonthlyReport/tables/t3_trees_by_planting_year.py
from core.libs import pd, np

DISPLAY_MAP  = {"CR": "Costa Rica", "GT": "Guatemala", "MX": "Mexico", "US": "USA"}
COUNTRIES    = ["Costa Rica", "Guatemala", "Mexico", "USA"]

def build_t3_trees_by_planting_year(mbt: pd.DataFrame) -> pd.DataFrame:
    """
    T3: Trees by Planting Year (Country x Year) con Status of Trees = {Contracted, Planted, Surviving}.
    - Entrada: MBT ya materializada.
    - Año de referencia: planting_year (si falta, intenta de planting_date; si no, cae a etp_year).
    - Contracted/Planted: seleccionados por allocation_type (ETP / COP / COP/ETP).
    - Surviving: cap(alive_sc, Planted_use).
    - Filtros base: status != 'Out of Program' y Filter != 'Omit' (como T2).
    - Survival % poblacional por planting_year = ΣSurviving / ΣPlanted (excluye OOP y Filter).
    """
    if mbt is None or mbt.empty:
        return pd.DataFrame(columns=["Year","Row", *COUNTRIES, "Total","Survival %","Survival_Summary"])

    df = mbt.copy()

    # ---------- Country legible ----------
    df["Country"] = df.get("region").replace(DISPLAY_MAP).fillna(df.get("region"))

    # ---------- Planting Year preferente ----------
    py = pd.to_numeric(df.get("planting_year"), errors="coerce").astype("Int64")
    if "planting_date" in df.columns:
        dt = pd.to_datetime(df["planting_date"], errors="coerce")
        py = py.fillna(dt.dt.year.astype("Int64"))
    py = py.fillna(pd.to_numeric(df.get("etp_year"), errors="coerce").astype("Int64"))
    df["planting_year"] = py

    # ---------- Filtros base (como T2) ----------
    df = df[df["status"].fillna("").str.strip() != "Out of Program"]
    if "Filter" in df.columns:
        df = df[df["Filter"].fillna("") != "Omit"]

    # ---------- Tipado de métricas ----------
    for c in [
        "contracted_etp","planted_etp","current_surviving_trees",
        "contracted_cop","planted_cop","surviving_cop",
        "trees_contract","alive_sc","planted"
    ]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    # ---------- allocation_type (si ya lo traes en MBT, úsalo; si no, derivamos simple) ----------
    # Esperado: 'ETP', 'COP', 'COP/ETP'
    at = df.get("allocation_type")
    if at is None:
        # fallback MUY simple por cohortes (ajústalo si tienes util oficial)
        y = pd.to_numeric(df.get("etp_year"), errors="coerce")
        alloc = np.where(y.isin([2015, 2017]), "COP",
                 np.where(y.isin([2019, 2020, 2021, 2022, 2023, 2024, 2025]), "ETP", "COP/ETP"))
        df["allocation_type"] = alloc
    else:
        df["allocation_type"] = at

    has_cop = df.get("has_cop")
    if has_cop is None:
        # si no viene, definimos "tiene COP" si hay alguna métrica COP > 0
        df["has_cop"] = ((df.get("contracted_cop",0) > 0) | (df.get("planted_cop",0) > 0))
    else:
        df["has_cop"] = has_cop.astype(bool)

    # ---------- Selección de métricas por allocation_type ----------
    # Contracted_use / Planted_use / Surviving_use
    df["Contracted_use"] = 0.0
    df["Planted_use"]    = 0.0
    df["Surviving_use"]  = 0.0

    is_cop  = df["allocation_type"].eq("COP")
    is_etp  = df["allocation_type"].eq("ETP")
    is_mix  = df["allocation_type"].eq("COP/ETP")

    # Contracted
    df.loc[is_etp, "Contracted_use"] = df.loc[is_etp, "contracted_etp"]
    df.loc[is_cop & df["has_cop"], "Contracted_use"] = df.loc[is_cop & df["has_cop"], "contracted_cop"]
    df.loc[is_mix, "Contracted_use"] = df.loc[is_mix, "contracted_cop"].where(df.loc[is_mix, "has_cop"],
                                                                               df.loc[is_mix, "contracted_etp"])
    # Planted
    df.loc[is_etp, "Planted_use"] = df.loc[is_etp, "planted_etp"]
    df.loc[is_cop & df["has_cop"], "Planted_use"] = df.loc[is_cop & df["has_cop"], "planted_cop"]
    df.loc[is_mix, "Planted_use"] = df.loc[is_mix, "planted_cop"].where(df.loc[is_mix, "has_cop"],
                                                                         df.loc[is_mix, "planted_etp"])

    # Surviving (cap con planted)
    base_surv = df.get("current_surviving_trees")
    if base_surv is None:
        # fallback si tu MBT separa por COP/ETP
        df["Surviving_use"] = np.where(
            is_cop | (is_mix & df["has_cop"]),
            df.get("surviving_cop", 0),
            df.get("alive_sc", 0)
        )
    else:
        df["Surviving_use"] = base_surv
    df["Surviving_use"] = df[["Surviving_use", "Planted_use"]].min(axis=1).clip(lower=0)

    # ---------- Aggregación por planting_year x Country ----------
    def _make_pivot(colname: str) -> pd.DataFrame:
        tmp = (
            df.groupby(["planting_year", "Country"], dropna=False)[colname]
              .sum(min_count=1)
              .reset_index()
              .pivot_table(index="planting_year", columns="Country", values=colname, aggfunc="sum", fill_value=0)
              .reset_index()
              .rename(columns={"planting_year": "Year"})
        )
        # asegúrate de todas las columnas de país
        for c in COUNTRIES:
            if c not in tmp.columns:
                tmp[c] = 0
        tmp["Total"] = tmp[COUNTRIES].sum(axis=1)
        return tmp[["Year", *COUNTRIES, "Total"]]

    piv_contracted = _make_pivot("Contracted_use")
    piv_planted    = _make_pivot("Planted_use")
    # survival y survival% usan el mismo filtrado base ya aplicado arriba
    piv_surv       = _make_pivot("Surviving_use")

    # ---------- Survival % poblacional por Year ----------
    # ΣSurviving / ΣPlanted
    rate = (
        piv_planted[["Year","Total"]].rename(columns={"Total":"P"})
        .merge(piv_surv[["Year","Total"]].rename(columns={"Total":"S"}), on="Year", how="outer")
        .fillna(0)
    )
    rate["Survival %"] = (rate["S"] / rate["P"]).where(rate["P"] > 0)

    # ---------- Survival Summary (no ponderado) ----------
    base_stats = df.copy()
    # en T3 usamos planting_year agrupado; ratio por contrato = Surviving_use / Contracted_use cuando Contracted_use>0
    with np.errstate(divide="ignore", invalid="ignore"):
        base_stats["survival_pct"] = (base_stats["Surviving_use"] /
                                      base_stats["Contracted_use"].replace(0, np.nan))
    summ = (
        base_stats.dropna(subset=["planting_year","survival_pct"])
        .groupby("planting_year")["survival_pct"]
        .apply(lambda s: None if s.empty else
               f"mean: {s.mean()*100:.1f}%, median: {s.median()*100:.1f}%, "
               f"max: {s.max()*100:.1f}%, min: {s.min()*100:.1f}%")
        .to_dict()
    )

    # ---------- Construcción de filas (loops) ----------
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
                    surv_pct = f"{round(float(s.iloc[0])*100,1)}%"
                surv_txt = summ.get(y, "") or ""
            r.append([int(rec["Year"]), row_name, *vals, total, surv_pct, surv_txt])
        return r

    rows = []
    rows += _rows_from_pivot(piv_contracted, "Contracted")
    rows += _rows_from_pivot(piv_planted,    "Planted")
    rows += _rows_from_pivot(piv_surv,       "Surviving")

    out = pd.DataFrame(rows, columns=["Year", "Row", *COUNTRIES, "Total", "Survival %", "Survival_Summary"])

    # 🔁 Renombra columna y ordena Year → Status of Trees (orden lógico CPS)
    out = out.rename(columns={"Row": "Status of Trees"})
    order = pd.Categorical(
        out["Status of Trees"],
        categories=["Contracted", "Planted", "Surviving"],
        ordered=True,
    )
    out = (
        out.assign(_ord=order)
        .sort_values(["Year", "_ord"])
        .drop(columns="_ord")
        .reset_index(drop=True)
    )

    # === 8) Footer de totales (después del ordenado para que queden al final) ===
    total_c = out.loc[out["Status of Trees"] == "Contracted", "Total"].sum()
    total_p = out.loc[out["Status of Trees"] == "Planted", "Total"].sum()
    total_s = out.loc[out["Status of Trees"] == "Surviving", "Total"].sum()

    footer = pd.DataFrame(
        [
            ["", "Total Contracted", *[""] * len(COUNTRIES), int(total_c), "", ""],
            ["", "Total Planted", *[""] * len(COUNTRIES), int(total_p), "", ""],
            ["", "Total Surviving", *[""] * len(COUNTRIES), int(total_s), "", ""],
        ],
        columns=["Year", "Status of Trees", *COUNTRIES, "Total", "Survival %", "Survival_Summary"],
    )

    out = pd.concat([out, footer], ignore_index=True)
    return out
