#InventoryMetrics/generate_helpers
from core.libs import re, pd, np

def safe_numeric(series):
    try:
        return pd.to_numeric(series, errors="coerce")
    except Exception:
        return pd.Series([None] * len(series), index=series.index)


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

            df.to_sql(target, engine, if_exists="append", index=False)
            print(f"📁 Generada: {target} ({len(df)} contratos)")

        except Exception as e:
            print(f"⚠️  No se pudo crear {target}: {e}")

def add_cruise_date_to_metrics(engine, df_metrics, country, year):
    # Lee la tabla catálogo (puedes parametrizar schema si aplica)
    cat_table = f"cat_inventory_{country}_{year}"
    cat = pd.read_sql(f"SELECT contractcode, cruisedate FROM {cat_table}", engine)
    cat.rename(columns={"contractcode": "contract_code", "cruisedate": "cruise_date"}, inplace=True)
    # Haz el merge (left, para no perder contratos)
    df_metrics = df_metrics.merge(cat, on="contract_code", how="left")
    # Si ya tienes una columna cruise_date y prefieres la del catálogo cuando no exista, haz fillna
    # df_metrics['cruise_date'] = df_metrics['cruise_date_x'].combine_first(df_metrics['cruise_date_y'])
    return df_metrics


def prefer_nonnull(series):
    """
    Elige el primer valor no nulo de la serie.
    """
    for val in series:
        if pd.notnull(val) and not (isinstance(val, float) and np.isnan(val)):
            return val
    return series.iloc[0]

def deduplicate_and_merge_metrics(df_full, keys=None):
    """
    Ordena y fusiona los registros de métricas, priorizando los registros con más datos calculados.
    Devuelve un DataFrame final listo para guardar.
    """
    if keys is None:
        keys = ["contract_code", "inventory_year", "inventory_date"]
    # 1. Ordena priorizando los que tengan total_trees, survival, mortality
    df_full = df_full.sort_values(
        by=["total_trees", "survival", "mortality"],
        ascending=[False, False, False]
    )
    # 2. Agrupa y fusiona
    df_final = df_full.groupby(keys, dropna=False).agg(prefer_nonnull).reset_index()
    # 3. Calcula pkid por si falta
    if "pkid" not in df_final.columns or df_final["pkid"].isnull().any():
        df_final["pkid"] = df_final["contract_code"].astype(str) + " " + df_final["inventory_year"].astype(str)
    # 4. (Opcional) Limpia progress
    df_final["progress"] = df_final.apply(
        lambda row: "OK" if pd.notnull(row.get("total_trees")) and pd.notnull(row.get("survival")) else "error", axis=1
    )
    return df_final

def fuse_rows(group):
    # Si solo hay una fila, regresa esa fila
    if len(group) == 1:
        return group.iloc[0]
    # Si hay más, recorre columna a columna y fusiona
    result = {}
    for col in group.columns:
        vals = group[col]
        # Elige el primer valor no nulo/ni vacío, si todos son nulos deja nulo
        value = next((v for v in vals if pd.notnull(v) and v != ''), None)
        result[col] = value
    return pd.Series(result)
