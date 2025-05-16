# inventory_metrics/generate.py

from core.libs import re, pd
from sqlalchemy import text
from tqdm import tqdm
from core.schema import get_column
from core.doyle_calculator import calculate_doyle
from db import get_engine  # asegúrate que esté bien importado

def get_inventory_tables(engine):
    """Busca todas las tablas que comienzan con 'inventory_' en public"""
    sql = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_name ILIKE 'inventory_%'
    """
    return [r[0] for r in engine.execute(text(sql))]

def get_cruise_date(engine, country, year, contract_code):
    """Obtiene la fecha de cruise desde cat_inventory_<country>_<year>"""
    table = f"cat_inventory_{country}_{year}"
    sql = f"""
        SELECT cruise_start_date
        FROM public.{table}
        WHERE "ContractCode" = :code
        LIMIT 1
    """
    result = engine.execute(text(sql), {"code": contract_code}).fetchone()
    return result[0] if result else None

def process_inventory_table(engine, table):
    df = pd.read_sql(f'SELECT * FROM public.{table}', engine)
    if df.empty:
        return []

    df = df.copy()
    df = df[df[get_column(df, "dbh_in")].notna()]
    df = df[df[get_column(df, "tht_ft")].notna()]

    contract_col = get_column(df, "contractcode")
    dbh_col = get_column(df, "dbh_in")
    tht_col = get_column(df, "tht_ft")
    mht_col = get_column(df, "merch_ht_ft")

    df["doyle_bf"] = df.apply(lambda row: calculate_doyle(row[dbh_col], row[tht_col]), axis=1)

    rows = []

    for contract_code, group in df.groupby(contract_col):
        country_year = re.findall(r"inventory_([a-z]+)_(\d{4})", table)[0]
        country, year = country_year
        year = int(year)

        cruise_date = get_cruise_date(engine, country, year, contract_code)

        live = group[group[get_column(df, "status_id")] == 1]  # asumimos 1 = Live

        if live.empty:
            continue

        row = {
            "id": f"{contract_code}-{year}",
            "contract_code": contract_code,
            "inventory_year": year,
            "inventory_date": cruise_date,
            "dbh_mean": round(live[dbh_col].mean(), 2),
            "dbh_stdv": round(live[dbh_col].std(), 2),
            "tht_mean": round(live[tht_col].mean(), 2),
            "tht_stdv": round(live[tht_col].std(), 2),
            "mht_mean": round(live[mht_col].mean(), 2),
            "mht_stdv": round(live[mht_col].std(), 2),
            "doyle_bf_mean": round(live["doyle_bf"].mean(), 2),
            "doyle_bf_stdv": round(live["doyle_bf"].std(), 2),
            "doyle_bf_total": round(live["doyle_bf"].sum(), 2),
        }

        rows.append(row)

    return rows

def upsert_metrics(engine, rows):
    if not rows:
        return

    df = pd.DataFrame(rows)

    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT INTO masterdatabase.inventory_metrics (
                    id, contract_code, inventory_year, inventory_date,
                    dbh_mean, dbh_stdv,
                    tht_mean, tht_stdv,
                    mht_mean, mht_stdv,
                    doyle_bf_mean, doyle_bf_stdv, doyle_bf_total
                ) VALUES (
                    :id, :contract_code, :inventory_year, :inventory_date,
                    :dbh_mean, :dbh_stdv,
                    :tht_mean, :tht_stdv,
                    :mht_mean, :mht_stdv,
                    :doyle_bf_mean, :doyle_bf_stdv, :doyle_bf_total
                )
                ON CONFLICT (id) DO UPDATE SET
                    dbh_mean = EXCLUDED.dbh_mean,
                    dbh_stdv = EXCLUDED.dbh_stdv,
                    tht_mean = EXCLUDED.tht_mean,
                    tht_stdv = EXCLUDED.tht_stdv,
                    mht_mean = EXCLUDED.mht_mean,
                    mht_stdv = EXCLUDED.mht_stdv,
                    doyle_bf_mean = EXCLUDED.doyle_bf_mean,
                    doyle_bf_stdv = EXCLUDED.doyle_bf_stdv,
                    doyle_bf_total = EXCLUDED.doyle_bf_total,
                    inventory_date = EXCLUDED.inventory_date
            """), row.to_dict())

def main():
    print("📊 Generando métricas de inventario...")
    engine = get_engine()
    tables = get_inventory_tables(engine)

    all_rows = []

    for table in tqdm(tables, desc="Procesando tablas"):
        rows = process_inventory_table(engine, table)
        all_rows.extend(rows)

    upsert_metrics(engine, all_rows)
    print("✅ Métricas actualizadas en masterdatabase.inventory_metrics")

if __name__ == "__main__":
    main()
