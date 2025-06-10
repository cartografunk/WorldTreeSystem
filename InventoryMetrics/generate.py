# inventory_metrics/generate.py

from tqdm import tqdm

from core.libs import re, pd, text
from core.db import get_engine
from core.schema_helpers import get_column


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


def get_cruise_date(df, contract_code, engine, country, year):
    table_name = f"cat_inventory_{country}_{year}"
    try:
        catalog = pd.read_sql(
            f'SELECT contractcode, cruisedate FROM {table_name}',
            engine
        )
        match = catalog[catalog["contractcode"] == contract_code]
        if not match.empty:
            date_val = match.iloc[0]["cruisedate"]
            return pd.to_datetime(date_val).strftime("%Y-%m-%d") if pd.notnull(date_val) else "pending"
        else:
            print(f"⚠️  No CruiseDate encontrado en {table_name} para {contract_code}")
            return "pending"
    except Exception as e:
        print(f"⚠️  Error accediendo a {table_name}: {e}")
        return "pending"


def safe_numeric(series):
    try:
        return pd.to_numeric(series, errors="coerce")
    except Exception:
        return pd.Series([None] * len(series), index=series.index)


def process_inventory_table(engine, table):
    df = pd.read_sql(f'SELECT * FROM public.{table}', engine)
    #print(f"\n📄 Columnas cargadas para {table}: {df.columns.tolist()}")
    if df.empty:
        return []

    df = df.copy()

    # Obtener columnas reales desde el esquema
    contract_col = get_column("contractcode", df)
    #print(f"✅ contract_col: {contract_col}")
    dbh_col = get_column("dbh_in", df)
    tht_col = get_column("tht_ft", df)
    mht_col = get_column("merch_ht_ft", df)
    dead_tree_col = get_column("dead_tree", df)
    alive_tree_col = get_column("alive_tree", df)
    doyle_col = get_column("doyle_bf", df)

    #print(f"✅ status_col: {status_col}")

    # Crear vista filtrada sin perder metadatos como CruiseDate
    filtered_df = df[
        df[dbh_col].notna() &
        df[tht_col].notna() &
        df[mht_col].notna() &
        df[doyle_col].notna() &
        df[dead_tree_col].notna() &
        df[alive_tree_col].notna() &
        df[doyle_col].notna()
    ]

    rows = []

    for contract_code, group in filtered_df.groupby(contract_col):

        total_alive = group["alive_tree"].sum()
        total_dead = group["dead_tree"].sum()
        total_trees = total_alive + total_dead
        survival = round((total_alive / total_trees) * 100, 2) if total_trees else 0.0
        mortality = round((total_dead / total_trees) * 100, 2) if total_trees else 0.0

        country_year = re.findall(r"inventory_([a-z]+)_(\d{4})", table)[0]
        country, year = country_year
        year = int(year)

        cruise_date = get_cruise_date(df, contract_code, engine, country, year)
        if cruise_date is None:
            cruise_date = "pending"  # default value when no date is available

        live = group  # ya no filtramos por árboles vivos

        row = {
            "contract_code": contract_code,
            "inventory_year": year,
            "inventory_date": cruise_date,
            "total_trees": total_trees,
            "survival": f"{survival}%",
            "mortality": f"{mortality}%",
            "dbh_mean": round(safe_numeric(live[dbh_col]).mean(), 2),
            "dbh_std": round(safe_numeric(live[dbh_col]).std(), 2),
            "tht_mean": round(safe_numeric(live[tht_col]).mean(), 2),
            "tht_std": round(safe_numeric(live[tht_col]).std(), 2),
            "mht_mean": round(safe_numeric(live[mht_col]).mean(), 2),
            "mht_std": round(safe_numeric(live[mht_col]).std(), 2),
            "doyle_bf_mean": round(safe_numeric(live[doyle_col]).mean(), 2),
            "doyle_bf_std": round(safe_numeric(live[doyle_col]).std(), 2),
            "doyle_bf_total": round(safe_numeric(live[doyle_col]).sum(), 2),
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

def create_cat_inventory_tables(engine, tables: list[str]):
    """
    Genera las tablas cat_inventory_<country>_<year> a partir de cada tabla inventory_<country>_<year>.
    """
    for table in tables:
        m = re.match(r"inventory_([a-z]+)_(\d{4})", table)
        if not m:
            continue
        country, year = m.groups()
        target = f"cat_inventory_{country}_{year}"

        try:
            df = pd.read_sql(f"""
                SELECT DISTINCT contractcode, cruisedate
                FROM public.{table}
                WHERE contractcode IS NOT NULL
            """, engine)

            df.to_sql(target, engine, if_exists="replace", index=False)
            print(f"📁 Generada: {target} ({len(df)} contratos)")

        except Exception as e:
            print(f"⚠️  No se pudo crear {target}: {e}")



if __name__ == "__main__":
    main()
