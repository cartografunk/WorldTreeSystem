# inventory_metrics/current_generator.py
"""
Regenerates masterdatabase.inventory_metrics_current table safely.
Now uses backup_manager + safe_ops for data protection.
"""

from core.db import get_engine
from core.libs import text
from core.backup_manager import backup_table
from core.safe_ops import safe_drop_table, safe_create_table_as

# SQL for the main SELECT statement (without DROP/CREATE)
SQL_SELECT_CURRENT = """
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
    cti.status AS contract_status,
    ROW_NUMBER() OVER (PARTITION BY im.contract_code ORDER BY im.inventory_year DESC) as rn
FROM masterdatabase.inventory_metrics im
LEFT JOIN masterdatabase.contract_tree_information cti
    ON im.contract_code = cti.contract_code
"""

# SQL for post-creation operations (view + insert)
SQL_POST_CREATE = """
-- Recrear la vista que depende de inventory_metrics_current
CREATE OR REPLACE VIEW masterdatabase.survival_current AS
SELECT 
    contract_code,
    inventory_year,
    survival,
    total_trees,
    mortality
FROM masterdatabase.inventory_metrics_current
WHERE rn = 1;

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
    cti.status,           -- contract_status
    1                     -- rn
FROM masterdatabase.contract_tree_information cti
LEFT JOIN masterdatabase.inventory_metrics_current imc
    ON cti.contract_code = imc.contract_code
WHERE imc.contract_code IS NULL;
"""


def regenerate_inventory_metrics_current(engine=None):
    """
    Regenera la tabla masterdatabase.inventory_metrics_current con la lógica WorldTree.

    ✅ SAFE VERSION: Usa backup_manager + safe_ops para protección de datos.

    Pasos:
    1. Crea backup de la tabla actual
    2. Drop seguro (verifica que backup existe)
    3. CREATE TABLE AS usando safe_create_table_as
    4. Ejecuta operaciones post-creación (view + insert)

    Args:
        engine: SQLAlchemy engine (si None, usa get_engine())
    """
    if engine is None:
        engine = get_engine()

    print("🔄 Regenerando masterdatabase.inventory_metrics_current ...")

    # PASO 1: Backup antes de cualquier operación destructiva
    print("🛡️  Creando backup de inventory_metrics_current...")
    try:
        backup_table(engine, "inventory_metrics_current", schema="masterdatabase")
    except Exception as e:
        print(f"⚠️  No se pudo crear backup (tabla puede no existir aún): {e}")

    # PASO 2: Drop seguro (verifica backup)
    print("🗑️  Eliminando tabla anterior de forma segura...")
    try:
        safe_drop_table(
            engine,
            "inventory_metrics_current",
            schema="masterdatabase",
            require_backup=False  # Ya hicimos backup arriba manualmente
        )
    except Exception as e:
        print(f"⚠️  No se pudo eliminar tabla (puede no existir): {e}")

    # PASO 3: Crear tabla nueva de forma segura
    print("📊 Creando nueva tabla inventory_metrics_current...")
    row_count = safe_create_table_as(
        engine,
        schema="masterdatabase",
        table="inventory_metrics_current",
        select_sql=SQL_SELECT_CURRENT
    )
    print(f"✅ Tabla creada con {row_count} filas")

    # PASO 4: Ejecutar operaciones post-creación
    print("🔧 Ejecutando operaciones post-creación (view + inserts)...")
    with engine.begin() as conn:
        for statement in SQL_POST_CREATE.split(';'):
            stmt = statement.strip()
            if stmt:
                conn.execute(text(stmt))

    print("✅ Tabla masterdatabase.inventory_metrics_current actualizada exitosamente")


if __name__ == "__main__":
    regenerate_inventory_metrics_current()