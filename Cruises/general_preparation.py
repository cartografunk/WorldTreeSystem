#Cruises/general_preparation.py

from core.db import get_engine
from core.libs import pd

from core.db import get_engine
from core.libs import pd

CRITICAL_FIELDS = ['tht_mean', 'dbh_mean', 'doyle_bf_mean']

def contracts_missing_metrics(year, tabla='masterdatabase.inventory_metrics'):
    engine = get_engine()
    sql = f"""
    SELECT contract_code, inventory_year, {', '.join(CRITICAL_FIELDS)}
    FROM {tabla}
    WHERE inventory_year = {year}
    """
    df = pd.read_sql(sql, engine)
    # Marca como incompleto si cualquiera de los campos críticos es NULL
    mask = df[CRITICAL_FIELDS].isnull().any(axis=1)
    return set(df.loc[mask, 'contract_code'].astype(str))

