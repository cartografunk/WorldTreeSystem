from sqlalchemy import text
from core.db import get_engine
from GeneradordeReportes.utils.helpers import get_inventory_table_name
from core.schema_helpers import get_column

def get_mortality_metrics(
    engine,
    country: str,
    year: int,
    contract_code: str
) -> dict:
    # Obtener número de árboles contratados desde cat_farmers
    with engine.connect() as conn:
        ct_sql = text(
            'SELECT contracted_trees FROM public.cat_farmers WHERE contractcode = :code'
        )
        ct_row = conn.execute(ct_sql, {"code": contract_code}).scalar_one_or_none()
    contract_trees = int(ct_row) if ct_row is not None else 0

    # Nombre de la tabla de inventario dinámico
    table_name = get_inventory_table_name(country, year)
    # Columnas según esquema (¡ya todo con get_column!)
    dead_col = get_column("dead_tree")
    alive_col = get_column("alive_tree")
    contract_col = get_column("contractcode")

    # Consultar sumas de muertos y vivos
    metrics_sql = f"""
        SELECT
            SUM({dead_col}) AS muertos,
            SUM({alive_col}) AS vivos
        FROM public.{table_name}
        WHERE {contract_col} = :code
        """
    with engine.connect() as conn:
        row = conn.execute(text(metrics_sql), {"code": contract_code}).mappings().one_or_none()

    dead = int(row.get('muertos') or 0)
    alive = int(row.get('vivos') or 0)
    sample_total = dead + alive if (dead + alive) > 0 else 1

    # Cálculo de tasas
    rate = dead / sample_total * 100
    dead_per_100 = round(rate)

    # Estimación de sobrevivientes
    survivors_estimated = int((alive / sample_total) * contract_trees)

    return {
        'dead': dead,
        'alive': alive,
        'rate': rate,
        'dead_per_100': dead_per_100,
        'survivors_estimated': survivors_estimated,
    }
