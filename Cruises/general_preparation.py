#Cruises/general_preparation.py

from core.db import get_engine
from core.libs import pd

METRIC_FIELDS = [
    "survival",
    "tht_mean", "tht_std",
    "mht_mean", "mht_std",
    "dbh_mean", "dbh_std",
    "doyle_bf_mean", "doyle_bf_std",
    "doyle_bf_total",
    "projected_dbh",
    "projected_doyle_bf"
]

def contracts_missing_metrics(year, threshold=5, tabla='masterdatabase.inventory_metrics'):
    """
    Devuelve un set de contract_code (o contract_code+year si tu PK es compuesta) que necesitan reprocesarse.
    """
    engine = get_engine()
    sql = f"""
    SELECT contract_code, inventory_year, {', '.join(METRIC_FIELDS)}
    FROM {tabla}
    WHERE inventory_year = {year}
    """
    df = pd.read_sql(sql, engine)
    df['num_nulls'] = df[METRIC_FIELDS].isnull().sum(axis=1)
    # Solo los que tienen >= threshold campos métricos nulos
    return set(df.loc[df['num_nulls'] >= threshold, 'contract_code'].astype(str))
