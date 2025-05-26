# inventory_metrics/generate.py

from tqdm import tqdm

from core.libs import re, pd, text
from core.db import get_engine
from Cruises.utils.cleaners import get_column


def get_inventory_tables(engine):
    """Busca todas las tablas que comienzan con 'inventory_' en public"""
    sql = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_name ILIKE 'inventory_%'
    """
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        return [row[0] for row in result]


def get_cruise_date(df, contract_code):
    contract_col = get_column(df, "contractcode")

    try:
        cruise_col = get_column(df, "cruisedate")
    except KeyError:
        print(f"⚠️ No se encontró 'cruisedate' para contrato {contract_code}.")
        return None

    filtered = df[df[contract_col] == contract_code]
    if filtered.empty or cruise_col not in filtered.columns:
        return None

    return filtered.sort_values(by=cruise_col, ascending=False).iloc[0][cruise_col]


def process_inventory_table(engine, table):
    df = pd.read_sql(f'SELECT * FROM public.{table}', engine)
    if df.empty:
        return []

    df = df.copy()

    # Obtener columnas reales desde el esquema
    contract_col = get_column(df, "contractcode")
    dbh_col = get_column(df, "dbh_in")
    tht_col = get_column(df, "tht_ft")
    mht_col = get_column(df, "merch_ht_ft")
    doyle_col = get_column(df, "doyle_bf")
    status_col = get_column(df, "status_id")

    # Crear vista filtrada sin perder metadatos como CruiseDate
    filtered_df = df[
        df[dbh_col].notna() &
        df[tht_col].notna() &
        df[mht_col].notna() &
        df[doyle_col].notna() &
        df[status_col].notna()
    ]

    rows = []

    for contract_code, group in filtered_df.groupby(contract_col):
        country_year = re.findall(r"inventory_([a-z]+)_(\d{4})", table)[0]
        country, year = country_year
        year = int(year)

        cruise_date = get_cruise_date(df, contract_code)
        if cruise_date is None:
            cruise_date = "pending"  # default value when no date is available

        live = group[group[status_col] == 1]

        if live.empty:
            continue

        row = {
            "contract_code": contract_code,
            "inventory_year": year,
            "inventory_date": cruise_date,
            "dbh_mean": round(live[dbh_col].mean(), 2),
            "dbh_std": round(live[dbh_col].std(), 2),
            "tht_mean": round(live[tht_col].mean(), 2),
            "tht_std": round(live[tht_col].std(), 2),
            "mht_mean": round(live[mht_col].mean(), 2),
            "mht_std": round(live[mht_col].std(), 2),
            "doyle_bf_mean": round(live[doyle_col].mean(), 2),
            "doyle_bf_std": round(live[doyle_col].std(), 2),
            "doyle_bf_total": round(live[doyle_col].sum(), 2),
        }

        rows.append(row)

    return rows


def upsert_metrics(engine, rows):
    if not rows:
        return

    df = pd.DataFrame(rows)

    # Asegurar que la tabla tenga la estructura correcta
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS masterdatabase.inventory_metrics (
                contract_code TEXT,
                inventory_year INTEGER,
                inventory_date TEXT,
                dbh_mean NUMERIC,
                dbh_std NUMERIC,
                tht_mean NUMERIC,
                tht_std NUMERIC,
                mht_mean NUMERIC,
                mht_std NUMERIC,
                doyle_bf_mean NUMERIC,
                doyle_bf_std NUMERIC,
                doyle_bf_total NUMERIC,
                PRIMARY KEY (contract_code, inventory_date)
            )
        """))

        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT INTO masterdatabase.inventory_metrics (
                    contract_code, inventory_year, inventory_date,
                    dbh_mean, dbh_std,
                    tht_mean, tht_std,
                    mht_mean, mht_std,
                    doyle_bf_mean, doyle_bf_std, doyle_bf_total
                ) VALUES (
                    :contract_code, :inventory_year, :inventory_date,
                    :dbh_mean, :dbh_std,
                    :tht_mean, :tht_std,
                    :mht_mean, :mht_std,
                    :doyle_bf_mean, :doyle_bf_std, :doyle_bf_total
                )
                ON CONFLICT (contract_code, inventory_date) DO UPDATE SET
                    dbh_mean = EXCLUDED.dbh_mean,
                    dbh_std = EXCLUDED.dbh_std,
                    tht_mean = EXCLUDED.tht_mean,
                    tht_std = EXCLUDED.tht_std,
                    mht_mean = EXCLUDED.mht_mean,
                    mht_std = EXCLUDED.mht_std,
                    doyle_bf_mean = EXCLUDED.doyle_bf_mean,
                    doyle_bf_std = EXCLUDED.doyle_bf_std,
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
