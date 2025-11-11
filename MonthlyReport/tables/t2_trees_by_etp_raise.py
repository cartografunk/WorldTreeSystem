# MonthlyReport/tables/t2_trees_by_etp_raise.py
from core.libs import pd, np

COUNTRY_COLS = ["Costa Rica", "Guatemala", "Mexico", "USA"]
REGION_TO_COUNTRY = {"CR": "Costa Rica", "GT": "Guatemala", "MX": "Mexico", "US": "USA"}
FULFILLED_CUTOFF = 2023  # <= 2023 => "Fulfilled"


def _apply_filter_by_metric(df, metric_type: str):
    """
    Filtra según el tipo de métrica:
    - "contracted": Excluye solo "Omit", incluye "Pending Planting"
    - "planted": Excluye "Omit" y "Pending Planting"
    - "surviving": Excluye "Omit" y "Pending Planting"

    Args:
        df: DataFrame a filtrar
        metric_type: "contracted" | "planted" | "surviving"

    Returns:
        DataFrame filtrado
    """
    if "Filter" not in df.columns:
        return df

    result = df.copy()

    if metric_type == "contracted":
        # Solo excluir "Omit", incluir "Pending Planting"
        result = result[result["Filter"].fillna("") != "Omit"]
    else:  # "planted" o "surviving"
        # Excluir tanto "Omit" como "Pending Planting"
        result = result[~result["Filter"].fillna("").isin(["Omit", "Pending Planting"])]

    return result


def build_etp_trees_table2(
        mbt: pd.DataFrame,
        so_by_year: dict | None = None,
) -> pd.DataFrame:
    """
    T2 (Trees by ETP) desde MBT + series_obligation opcional.
    Filtros por métrica:
      - Contracted: Excluye solo "Omit" (incluye "Pending Planting")
      - Planted/Surviving: Excluye "Omit" y "Pending Planting"
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

    # Métricas base (sin filtrar aún)
    df["value__Contracted"] = pd.to_numeric(df.get("contracted_etp", 0), errors="coerce").fillna(0)
    df["value__Planted"] = pd.to_numeric(df.get("planted_etp", 0), errors="coerce").fillna(0)
    df["value__Surviving"] = pd.to_numeric(df.get("current_surviving_trees", 0), errors="coerce").fillna(0)

    rows = []
    for (y, t), g in df.groupby(["etp_year", "Type of ETP"], dropna=True):
        for status, col in [
            ("Contracted", "value__Contracted"),
            ("Planted", "value__Planted"),
            ("Surviving", "value__Surviving"),
        ]:
            # 🆕 Aplicar filtro específico por métrica
            metric_type = status.lower()
            g_filtered = _apply_filter_by_metric(g, metric_type)

            metric = g_filtered.groupby("Country", dropna=False)[col].sum(min_count=1)
            vals = {c: float(metric.get(c, 0.0) if c in metric.index else 0.0) for c in COUNTRY_COLS}
            total = float(sum(vals.values()))

            survival_summary = None
            obligation_remaining = None

            if status == "Surviving":
                # Survival by Contracts Summary (usa g_filtered que ya excluye Pending Planting)
                contr = pd.to_numeric(g_filtered.get("trees_contract", 0), errors="coerce").fillna(0)
                surv = pd.to_numeric(g_filtered.get("current_surviving_trees", 0), errors="coerce").fillna(0)
                with np.errstate(divide="ignore", invalid="ignore"):
                    ratio = (surv / contr).replace([np.inf, -np.inf], np.nan)
                s = ratio.dropna()
                if not s.empty:
                    def pct(v): return f"{v * 100:.1f}%"

                    mean_v, med_v = s.mean(), s.median()
                    mode_v = ((s * 100).round(1).value_counts().idxmax() / 100.0) if not s.empty else np.nan
                    max_v, min_v = s.max(), s.min()
                    rng_v = max_v - min_v
                    survival_summary = (
                        f"mean: {pct(mean_v)}, median: {pct(med_v)}, "
                        f"mode: {pct(mode_v) if pd.notna(mode_v) else 'NA'}, "
                        f"max: {pct(max_v)}, min: {pct(min_v)}, range: {pct(rng_v)}"
                    )

                # Obligation Remaining (usa contracted del grupo SIN "Pending Planting")
                # Nota: Para Obligation usamos el filtro de "contracted" que SÍ incluye Pending Planting
                g_for_obligation = _apply_filter_by_metric(g, "contracted")
                contracted_total_etp = float(
                    pd.to_numeric(g_for_obligation.get("contracted_etp", 0), errors="coerce").fillna(0).sum())
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