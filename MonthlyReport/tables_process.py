#MonthlyReport/tables_process.py

from core.libs import pd

def weighted_mean(df, value_col, weight_col):
    valid = df[weight_col] > 0
    if valid.sum() == 0:
        return None
    return (df.loc[valid, value_col] * df.loc[valid, weight_col]).sum() / df.loc[valid, weight_col].sum()

def get_allocation_type(etp_year):
    if pd.isna(etp_year):
        return []

    if etp_year in [2015, 2017]:
        return ['COP']
    elif etp_year in [2016, 2018]:
        return ['COP', 'ETP']
    else:
        return ['ETP']


def build_etp_trees(engine, etp_year, allocation_type):
    ca = pd.read_sql("SELECT * FROM masterdatabase.contract_allocation", engine)
    cti = pd.read_sql("SELECT * FROM masterdatabase.contract_trees_info", engine)

    if allocation_type == "COP":
        df = ca[ca["etp_year"] == etp_year]
        df_grouped = df.groupby("region")["total_can_allocation"].sum().reset_index()
        df_grouped.rename(columns={"total_can_allocation": "Total Trees"}, inplace=True)

    elif allocation_type == "ETP":
        df = cti[cti["etp_year"] == etp_year]
        df_grouped = df.groupby("region")["planted"].sum().reset_index()
        df_grouped.rename(columns={"planted": "Total Trees"}, inplace=True)

    elif allocation_type == "COP/ETP":
        df = ca[ca["etp_year"] == etp_year]
        df["combined_planted"] = df[["total_can_allocation", "usa_trees_planted"]].fillna(0).sum(axis=1)
        df_grouped = df.groupby("region")["combined_planted"].sum().reset_index()
        df_grouped.rename(columns={"combined_planted": "Total Trees"}, inplace=True)

    else:
        raise ValueError("allocation_type debe ser COP, ETP o COP/ETP")

    return df_grouped

def get_survival_data(engine):
    df = pd.read_sql("""
        SELECT cti.etp_year, cti.region, imc.survival
        FROM masterdatabase.inventory_metrics_current imc
        JOIN masterdatabase.contract_tree_information cti
            ON imc.contract_code = cti.contract_code
        WHERE imc.survival IS NOT NULL
    """, engine)

    df["Survival"] = (df["survival"] * 100).round(1).astype(str) + "%"
    df["etp_year"] = df["etp_year"].astype("Int64")
    return df[["etp_year", "region", "Survival"]]



def format_survival_summary(df_survival):
    """
    Devuelve un resumen de supervivencia por etp_year como string con media, mediana y rango.
    """
    df = df_survival.copy()
    df["Survival_pct"] = pd.to_numeric(df["Survival"].str.replace("%", ""), errors="coerce") / 100

    resumen = df.groupby("etp_year")["Survival_pct"].agg(
        mean=lambda x: f"{x.mean():.1%}",
        median=lambda x: f"{x.median():.1%}",
        range=lambda x: f"{(x.max()-x.min()):.1%}"
    ).reset_index()

    resumen["Resumen"] = resumen.apply(
        lambda row: f"Mean: {row['mean']}, Median: {row['median']}, Range: {row['range']}", axis=1
    )
    return resumen[["etp_year", "Resumen"]]
