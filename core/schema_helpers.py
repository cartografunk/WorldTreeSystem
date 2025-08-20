#core/schema_helpers

from core.libs import pd, unicodedata, re
from core.schema import COLUMNS

from sqlalchemy import Text, Float, Numeric, Integer, DateTime, SmallInteger, Date

def rename_columns_using_schema(df):
  rename_map = {}

  for col_def in COLUMNS:
    logical = col_def["key"]
    candidates = [col_def["sql_name"]] + col_def.get("aliases", [])

    for alias in candidates:
      for real_col in df.columns:
        if clean_column_name(real_col) == clean_column_name(alias):
          rename_map[real_col] = logical
          break

  df = df.rename(columns=rename_map)
  return df


SQLALCHEMY_DTYPES = {
    "TEXT": Text(),
    "FLOAT": Float(),
    "NUMERIC": Numeric(),
    "INT": Integer(),
    "DATE": Date(),
    "SMALLINT": SmallInteger(),
}


def get_dtypes_for_dataframe(df):
  mapping = {}
  for col in COLUMNS:
    sa_type = SQLALCHEMY_DTYPES.get(col.get("dtype"))
    if not sa_type:
      continue

    # 1️⃣  Prefiere la clave lógica si está presente
    if col["key"] in df.columns:
      mapping[col["key"]] = sa_type
    # 2️⃣  Si no, usa el nombre SQL (lo que realmente llega a la BD)
    elif col["sql_name"] in df.columns:
      mapping[col["sql_name"]] = sa_type
  return mapping


_SA_TO_PD = {
    Text:          "string",
    Float:         "float64",
    Numeric:       "float64",
    Integer:       "Int64",        # entero nullable
    SmallInteger:  "Int16",
    DateTime:          "datetime64[ns]",
}


def clean_column_name(name):
  name = str(name)
  name = re.sub(r'[#\s]+', '_', name)
  name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('ascii')
  name = re.sub(r'[^\w_]', '', name)
  name = name.strip('_').lower()
  name = re.sub(r'_+', '_', name)
  return name


def get_column(logical_name: str, df: pd.DataFrame = None) -> str:
  """
  Si `df` es None → devuelve la clave lógica estándar (`key`) desde schema.
  Si `df` se proporciona → busca el nombre real en el DataFrame según aliases definidos.
  """
  for entry in COLUMNS:
    if logical_name == entry["key"] or logical_name == entry["sql_name"] or logical_name in entry.get("aliases", []):
      candidates = [entry["key"], entry["sql_name"]] + entry.get("aliases", [])
      break
  else:
    raise KeyError(f"❌ '{logical_name}' no está definido en schema")

  if df is None:
    return entry["key"]  # ← caso moderno: solo dame el nombre estandarizado

  # ← caso legacy: dime cómo se llama realmente en este df
  for candidate in candidates:
    if candidate in df.columns:
      return candidate

  raise KeyError(
    f"❌ No se encontró una columna para '{logical_name}' en df. Aliases probados: {candidates}"
  )


SQL_COLUMNS = { col["key"]: col["sql_name"] for col in COLUMNS }
FINAL_ORDER = [
    "contractcode",
    "farmername",
    "cruisedate",
    "id",
    "id_error",
    "stand",
    "plot",
    "plot_coordinate",
    "tree_number",
    "defect_ht_ft",
    "dbh_in",
    "tht_ft",
    "merch_ht_ft",
    "short_note",
    "Species_id",
    "Defect_id",
    "Pests_id",
    "Coppiced_id",
    "Permanent Plot_id",
    "Disease_id",
    "doyle_bf",
    "dead_tree",
    "alive_tree"
]

DTYPES = {
    col["sql_name"]: col["dtype"]
    for col in COLUMNS
    if "dtype" in col
}


