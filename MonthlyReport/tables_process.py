#MonthlyReport/tables_process.py

from core.libs import pd

def weighted_mean(df, value_col, weight_col):
    valid = df[weight_col] > 0
    if valid.sum() == 0:
        return None
    return (df.loc[valid, value_col] * df.loc[valid, weight_col]).sum() / df.loc[valid, weight_col].sum()

def get_allocation_type(etp_year):
    if etp_year in [2015, 2017]:
        return ['COP']
    elif etp_year in [2016, 2018]:
        return ['COP', 'ETP']
    else:
        return ['ETP']

def apply_allocation_split(df, alloc, allocation_type):
    # Elige el campo de porcentaje según allocation_type
    allocation_field = "canada_allocation_pct" if allocation_type == "COP" else "usa_allocation_pct"

    # Haz el merge
    df = df.merge(alloc[["contract_code", allocation_field]], on="contract_code", how="left")

    # Aplica el split a los campos que quieras (contract, planted, surviving...)
    for col in ["trees_contract", "planted"]:
        df[col] = df[col] * (df[allocation_field] / 100.0)

    # Si tienes surviving, igual
    if "surviving" in df.columns:
        df["surviving"] = df["surviving"] * (df[allocation_field] / 100.0)

    return df


