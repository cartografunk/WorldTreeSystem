from core.libs import pd
from core.db import get_engine
from MonthlyReport.tables_process import weighted_mean, apply_allocation_split

def build_etp_summary(year, allocation_type=None):
    engine = get_engine()
    cti = pd.read_sql(
        f"""
           SELECT 
               cti.contract_code, 
               cti.etp_year, 
               cti.region,
               cti.trees_contract,
               cti.planted
           FROM masterdatabase.contract_tree_information AS cti
           WHERE cti.etp_year = {year}
        """, engine
    )

    if allocation_type in ["COP", "ETP"]:
        alloc = pd.read_sql(
            "SELECT contract_code, canada_allocation_pct, usa_allocation_pct FROM masterdatabase.contract_allocation",
            engine
        )

        cti = apply_allocation_split(cti, alloc, allocation_type)

    # Pivot Contracted
    contracted = cti.pivot_table(index='etp_year', columns='region', values='trees_contract', aggfunc='sum', fill_value=0)
    contracted['Type'] = 'Contracted'

    # Pivot Planted
    planted = cti.pivot_table(index='etp_year', columns='region', values='planted', aggfunc='sum', fill_value=0)
    planted['Type'] = 'Planted'

    # Surviving (trae de metrics, join por contract_code, calcula por región)
    metrics = pd.read_sql(
        f"""
        SELECT contract_code, total_trees, survival
        FROM masterdatabase.inventory_metrics
        WHERE inventory_year = {year}
        """, engine
    )
    metrics["surviving"] = metrics["total_trees"] * (metrics["survival"].str.replace('%','').astype(float) / 100)
    cti = cti.merge(metrics[['contract_code', 'surviving']], on='contract_code', how='left')
    surviving = cti.pivot_table(index='etp_year', columns='region', values='surviving', aggfunc='sum', fill_value=0)
    surviving['Type'] = 'Surviving'

    # Junta todo
    tabla = pd.concat([contracted, planted, surviving])
    tabla.reset_index(inplace=True)
    # Ordena columnas (puedes personalizar)
    order_cols = ['etp_year', 'Type'] + [c for c in tabla.columns if c not in ['etp_year', 'Type']]
    tabla = tabla[order_cols]

    return tabla

if __name__ == "__main__":
    year = 2018
    allocation_type = "COP"
    df = build_etp_summary(year, allocation_type)
    print(df)
    df.to_excel(f"etp_summary_{year}_{allocation_type}.xlsx", index=False)
