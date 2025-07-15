from core.db import get_engine
from core.libs import text

def set_year_dates_to_oct_1():
    year = 2022
    engine = get_engine()
    TARGET_DATE = f'{year}-10-01'
    pattern = f"^inventory_.*_{year}$"

    with engine.begin() as conn:
        # 1. Actualiza en masterdatabase.inventory_metrics
        conn.execute(text("""
            UPDATE masterdatabase.inventory_metrics
            SET inventory_date = :date
            WHERE inventory_year = :year
        """), {'date': TARGET_DATE, 'year': year})

        # 2. Actualiza en masterdatabase.inventory_metrics_current
        conn.execute(text("""
            UPDATE masterdatabase.inventory_metrics_current
            SET inventory_date = :date
            WHERE inventory_year = :year
        """), {'date': TARGET_DATE, 'year': year})

        # 3. Actualiza en todas las tablas de inventario público de year
        result = conn.execute(
            text("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name ~ :pattern
            """),
            {'pattern': pattern}
        )
        for (table_name,) in result:
            print(f"Actualizando cruise_date en {table_name}...")
            try:
                conn.execute(
                    text(f"UPDATE public.{table_name} SET cruise_date = :date"),
                    {'date': TARGET_DATE}
                )
            except Exception as e:
                print(f"⚠️  No se pudo actualizar {table_name}: {e}")

    print(f"✅ Todas las fechas de inventarios {year} cambiadas a 01/Oct/{year}.")

if __name__ == "__main__":
    set_year_dates_to_oct_1()
