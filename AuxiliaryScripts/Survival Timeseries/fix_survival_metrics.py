# scripts/update_survival_metrics.py

from core.db import get_engine
from core.libs import pd
from sqlalchemy import text
import re

def run():
    engine = get_engine()

    # 1️⃣ Obtener todas las tablas tipo inventory_<country>_<year>
    tables_df = pd.read_sql("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name ~ '^inventory_[a-z]+_[0-9]{4}$'
    """, engine)

    for table in tables_df["table_name"]:
        match = re.match(r"inventory_([a-z]+)_(\d{4})", table)
        if not match:
            continue
        country_code, year = match.groups()
        print(f"🔁 Procesando {table} para {year.upper()}...")

        sql = f"""
            WITH counts AS (
                SELECT
                    contractcode AS contract_code,
                    SUM(dead_tree) AS total_dead,
                    SUM(alive_tree) AS total_alive
                FROM public.{table}
                GROUP BY contractcode
            )
            UPDATE masterdatabase.inventory_metrics m
            SET
                survival = ROUND(c.total_alive::NUMERIC / NULLIF((c.total_alive + c.total_dead)::NUMERIC, 0), 4),
                mortality = ROUND(c.total_dead::NUMERIC / NULLIF((c.total_alive + c.total_dead)::NUMERIC, 0), 4)
            FROM counts c
            WHERE
                m.contract_code = c.contract_code
                AND m.inventory_year = {year};
        """
        with engine.begin() as conn:
            conn.execute(text(sql))
        print(f"✅ Actualizado: {table}")

    print("🎯 Todos los survival y mortality han sido recalculados.")

if __name__ == "__main__":
    run()
