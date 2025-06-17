#InventoryMetrics/Inventory retriever
from core.libs import pd, text


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
