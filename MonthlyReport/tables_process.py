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

def build_etp_trees(engine, etp_year, allocation_type):
    alloc = pd.read_sql("SELECT * FROM masterdatabase.contract_allocation", engine)

    if allocation_type == "COP":
        df = alloc[alloc['etp_year'] == etp_year]
        df_grouped = df.groupby("region")["total_can_allocation"].sum().reset_index()
        df_grouped.rename(columns={"total_can_allocation": "Total Trees"}, inplace=True)
    elif allocation_type == "ETP":
        df = alloc[alloc['etp_year'] == etp_year]
        df_grouped = df.groupby("region")["usa_trees_planted"].sum().reset_index()
        df_grouped.rename(columns={"usa_trees_planted": "Total Trees"}, inplace=True)
    else:
        raise ValueError("allocation_type debe ser COP o ETP")

    return df_grouped


