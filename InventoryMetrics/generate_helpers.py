#InventoryMetrics/generate_helpers
from core.libs import re, pd, np

def safe_numeric(series):
    try:
        return pd.to_numeric(series, errors="coerce")
    except Exception:
        return pd.Series([None] * len(series), index=series.index)


def create_cat_inventory_tables(engine, tables: list[str], where: str = None):
    """
    Genera las tablas cat_inventory_<country>_<year> a partir de cada tabla inventory_<country>_<year>.
    Permite filtrar usando un WHERE SQL (por ejemplo, solo contratos de Guatemala).
    """
    for table in tables:
        m = re.match(r"inventory_([a-z]+)_(\d{4})", table)
        if not m:
            continue
        country, year = m.groups()
        target = f"cat_inventory_{country}_{year}"

        try:
            base_query = f"""
                SELECT DISTINCT contractcode, cruisedate
                FROM public.{table}
                WHERE contractcode IS NOT NULL
            """
            # Si se pasa un filtro extra, agréguelo al WHERE (con AND)
            if where and where.strip() != "1=1":
                base_query = base_query.rstrip() + f" AND {where}\n"

            df = pd.read_sql(base_query, engine)

            # Antes de guardar, filtra los nulos
            df = df[df['cruisedate'].notnull()]
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

def fuse_rows(group):
    # Fusiona columna a columna: toma el primer valor no nulo (útil) de cada campo.
    if len(group) == 1:
        return group.iloc[0]
    result = {}
    for col in group.columns:
        vals = group[col]
        value = next((v for v in vals if pd.notnull(v) and v != ''), None)
        result[col] = value
    return pd.Series(result)

def clean_and_fuse_metrics(df_full):
    """
    Toma el DataFrame combinado (df_full), fusiona filas duplicadas por clave,
    asegura columnas del schema y rellena campos faltantes.
    """
    # Homogeneiza nombres de columna (cruise_date → inventory_date)
    if "cruise_date" in df_full.columns:
        df_full["inventory_date"] = df_full["cruise_date"]
        df_full = df_full.drop(columns=["cruise_date"])

    # if "inventory_date" in df_full.columns:
    #     print("🔎 inventory_date types:")
    #     print(df_full["inventory_date"].apply(type).value_counts())
    #     print("🔎 Ejemplos de valores:", df_full["inventory_date"].unique()[:10])

    # Llaves de unicidad (ajusta si tu lógica de pipeline lo requiere)
    keys = ["contract_code", "inventory_year", "inventory_date"]
    df_final = df_full.groupby(keys, dropna=False).apply(fuse_rows).reset_index(drop=True)

    # Calcula pkid si falta
    if "pkid" not in df_final.columns or df_final["pkid"].isnull().any():
        df_final["pkid"] = df_final["contract_code"].astype(str) + " " + df_final["inventory_year"].astype(str)

    # Recalcula progress
    df_final["progress"] = df_final.apply(
        lambda row: "OK" if pd.notnull(row.get("total_trees")) and pd.notnull(row.get("survival")) else "error", axis=1
    )

    # Normaliza y evita columnas duplicadas
    if 'planting_year_y' in df_final.columns:
        df_final['planting_year'] = df_final['planting_year_y']
    if 'planting_date_y' in df_final.columns:
        df_final['planting_date'] = df_final['planting_date_y']
    for col in ['planting_year_x', 'planting_year_y', 'planting_date_x', 'planting_date_y']:
        if col in df_final.columns:
            df_final.drop(col, axis=1, inplace=True)

    # **Asegura todas las columnas del schema en orden correcto**
    schema = [
        "rel_path", "contract_code", "planting_year", "tree_age", "inventory_year", "inventory_date", "survival",
        "tht_mean", "tht_std", "mht_mean", "mht_std", "mht_pct_of_target",
        "dbh_mean", "dbh_std", "noncom_dbh_count", "dbh_pct_of_target", "doyle_bf_mean", "doyle_bf_std",
        "doyle_bf_total", "projected_dbh", "projected_doyle_bf", "pkid", "progress",
        "total_trees", "mortality"
    ]
    # Asegura que todas estén, si no existen, rellena con None
    for col in schema:
        if col not in df_final.columns:
            df_final[col] = None
    # Reordena
    df_final = df_final[schema]
    return df_final
