# inventory_metrics/current_generator.py

from core.db import get_engine, backup_table
from core.libs import text

SQL_REGENERATE = """
DROP TABLE IF EXISTS masterdatabase.inventory_metrics_current;

CREATE TABLE masterdatabase.inventory_metrics_current AS
SELECT
    im.rel_path,
    im.contract_code,
    im.planting_year,
    im.tree_age,
    im.inventory_year,
    im.inventory_date,
    im.survival,
    im.tht_mean,
    im.tht_std,
    im.mht_mean,
    im.mht_std,
    im.mht_pct_of_target,
    im.dbh_mean,
    im.dbh_std,
    im.noncom_dbh_count,
    im.dbh_pct_of_target,
    im.doyle_bf_mean,
    im.doyle_bf_std,
    im.doyle_bf_total,
    im.projected_dbh,
    im.projected_doyle_bf,
    im.pkid,
    im.progress,
    im.total_trees,
    im.mortality,
    im.planting_date,
    im.type_of_metric,
    cfi.status AS contract_status,
    ROW_NUMBER() OVER (PARTITION BY im.contract_code ORDER BY im.inventory_year DESC) as rn
FROM masterdatabase.inventory_metrics im
LEFT JOIN masterdatabase.contract_farmer_information cfi
    ON im.contract_code = cfi.contract_code;

-- Asegura que todos los contratos existan en metrics_current (aunque sean NULL)
INSERT INTO masterdatabase.inventory_metrics_current (
    rel_path, contract_code, planting_year, tree_age, inventory_year, inventory_date, survival,
    tht_mean, tht_std, mht_mean, mht_std, mht_pct_of_target,
    dbh_mean, dbh_std, noncom_dbh_count, dbh_pct_of_target,
    doyle_bf_mean, doyle_bf_std, doyle_bf_total,
    projected_dbh, projected_doyle_bf,
    pkid, progress, total_trees, mortality,
    planting_date, type_of_metric, contract_status, rn
)
SELECT
    NULL,                 -- rel_path
    cti.contract_code,    -- contract_code
    NULL,                 -- planting_year
    NULL,                 -- tree_age
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
    NULL,                 -- planting_date
    NULL,                 -- type_of_metric
    cfi.status,           -- contract_status
    1                     -- rn
FROM masterdatabase.contract_tree_information cti
LEFT JOIN masterdatabase.contract_farmer_information cfi
    ON cti.contract_code = cfi.contract_code
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
