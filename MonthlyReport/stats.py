# MonthlyReport/stats.py
from core.libs import pd

def _mode_safe(s: pd.Series):
    m = s.mode(dropna=True)
    return m.iloc[0] if not m.empty else pd.NA

def _percent_fmt(p):
    try:
        return f"{round(float(p) * 100, 1)}%" if pd.notna(p) else "N/A"
    except Exception:
        return "N/A"

def survival_stats(df: pd.DataFrame,
                   group_col: str,
                   survival_pct_col: str = "survival_pct"):
    """
    Calcula stats por grupo usando promedio simple:
      - mean, median, mode, max, min, range
    Devuelve:
      - stats_num: DataFrame con columnas numéricas
      - stats_txt: DataFrame con Survival_Summary formateado
    """
    stats_num = (
        df.dropna(subset=[survival_pct_col])
          .groupby(group_col)[survival_pct_col]
          .agg(mean="mean", median="median", mode=_mode_safe, max="max", min="min")
          .reset_index()
    )

    if not stats_num.empty:
        stats_num["range"] = stats_num["max"] - stats_num["min"]

        stats_txt = stats_num[[group_col]].copy()
        stats_txt["Survival_Summary"] = (
            "mean: "   + stats_num["mean"].apply(_percent_fmt)   + ", " +
            "median: " + stats_num["median"].apply(_percent_fmt) + ", " +
            "mode: "   + stats_num["mode"].apply(_percent_fmt)   + ", " +
            "max: "    + stats_num["max"].apply(_percent_fmt)    + ", " +
            "min: "    + stats_num["min"].apply(_percent_fmt)    + ", " +
            "range: "  + stats_num["range"].apply(_percent_fmt)
        )
    else:
        stats_txt = pd.DataFrame(columns=[group_col, "Survival_Summary"])

    return stats_num, stats_txt
