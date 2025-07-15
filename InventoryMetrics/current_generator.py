# inventory_metrics/current_generator.py

from core.db import get_engine, backup_table
from core.libs import text

SQL_REGENERATE = """
DROP TABLE IF EXISTS masterdatabase.inventory_metrics_current;

CREATE TABLE masterdatabase.inventory_metrics_current AS
SELECT
    rel_path,
    contract_code,
    NULL::TEXT AS type_of_metric,
    NULL::TEXT AS contract_status,
    inventory_year,
    inventory_date,
    survival,
    tht_mean,
    tht_std,
    mht_mean,
    mht_std,
    mht_pct_of_target,
    dbh_mean,
    dbh_std,
    noncom_dbh_count,
    dbh_pct_of_target,
    doyle_bf_mean,
    doyle_bf_std,
    doyle_bf_total,
    projected_dbh,
    projected_doyle_bf,
    pkid,
    progress,
    total_trees,
    mortality,
    rn
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY contract_code ORDER BY inventory_year DESC) as rn
    FROM masterdatabase.inventory_metrics
) sub
WHERE rn = 1;

-- Asegura que todos los contratos existan en metrics_current (aunque sean NULL)
INSERT INTO masterdatabase.inventory_metrics_current (
    rel_path, contract_code, type_of_metric, contract_status, inventory_year, inventory_date, survival,
    tht_mean, tht_std, mht_mean, mht_std, mht_pct_of_target,
    dbh_mean, dbh_std, noncom_dbh_count, dbh_pct_of_target,
    doyle_bf_mean, doyle_bf_std, doyle_bf_total,
    projected_dbh, projected_doyle_bf,
    pkid, progress, total_trees, mortality, rn
)
SELECT
    NULL,                 -- rel_path
    cti.contract_code,    -- contract_code
    NULL,                 -- type_of_metric
    NULL,                 -- contract_status
    NULL,                 -- inventory_year
    NULL,                 -- inventory_date
    NULL,                 -- survival
    NULL,                 -- tht_mean
    NULL,                 -- tht_std
    NULL,                 -- mht_mean
    NULL,                 -- mht_std
    NULL,                 -- mht_pct_of_target
    NULL,                 -- dbh_mean
    NULL,                 -- dbh_std
    NULL,                 -- noncom_dbh_count
    NULL,                 -- dbh_pct_of_target
    NULL,                 -- doyle_bf_mean
    NULL,                 -- doyle_bf_std
    NULL,                 -- doyle_bf_total
    NULL,                 -- projected_dbh
    NULL,                 -- projected_doyle_bf
    NULL,                 -- pkid
    NULL,                 -- progress
    NULL,                 -- total_trees
    NULL,                 -- mortality
    1                     -- rn
FROM masterdatabase.contract_tree_information cti
LEFT JOIN masterdatabase.inventory_metrics_current imc
  ON cti.contract_code = imc.contract_code
WHERE imc.contract_code IS NULL;
"""

def regenerate_inventory_metrics_current(engine=None, sql_code=SQL_REGENERATE):
    """
    Regenera la tabla masterdatabase.inventory_metrics_current con la lógica WorldTree.
    Si no se pasa engine, lo obtiene con get_engine().
    """
    if engine is None:
        engine = get_engine()
    # 🛡️ Backup antes de reemplazar
    backup_table("inventory_metrics_current")
    print("🔄 Regenerando masterdatabase.inventory_metrics_current ...")
    with engine.begin() as conn:
        for statement in sql_code.split(';'):
            stmt = statement.strip()
            if stmt:
                conn.execute(text(stmt))
    print("✅ Tabla masterdatabase.inventory_metrics_current actualizada.")

if __name__ == "__main__":
    regenerate_inventory_metrics_current()
