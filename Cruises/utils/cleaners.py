# utils/cleaners.py
from utils.libs import pd, unicodedata, re
from utils.column_mapper import COLUMN_LOOKUP
from utils.schema import COLUMNS


def get_column(df, logical_name):
    """
    Dado un nombre lógico (p.ej. 'Tree #', 'Status', 'Plot #', etc.),
    busca en schema.COLUMNS la entrada que lo contenga en su lista de aliases
    y devuelve el nombre exacto de la columna en `df.columns`.
    """

    # 1) encuentra la definición en schema.COLUMNS

    schema_entry = None

    for col in COLUMNS:

        if (logical_name == col["key"]
            or logical_name == col["sql_name"]
            or logical_name in col["aliases"]):
            schema_entry = col
            break

    if schema_entry is None:
        raise KeyError(f"❌ No hay entrada de esquema para '{logical_name}'")

    # 2) intenta coincidencia exacta contra df.columns
    df_cols = list(df.columns)
    for candidate in [schema_entry["key"], schema_entry["sql_name"]] + schema_entry["aliases"]:
        if candidate in df_cols:
            return candidate

    # 3) intenta coincidencia normalizada
    normalized_df = {clean_column_name(c): c for c in df_cols}

    for candidate in [schema_entry["key"], schema_entry["sql_name"]] + schema_entry["aliases"]:
        norm = clean_column_name(candidate)
        if norm in normalized_df:
                return normalized_df[norm]


    raise KeyError(
        f"❌ Columna lógica '{logical_name}' no encontrada en DataFrame. "
        f"Probados: key='{schema_entry['key']}', sql_name='{schema_entry['sql_name']}', "
        f"aliases={schema_entry['aliases']}"
    )


def clean_column_name(name: str) -> str:
    """Normaliza nombres preservando compatibilidad con UTF-8."""
    name = str(name)
    # Normalizar caracteres unicode (ej: Á → A + ´)
    name = unicodedata.normalize("NFKD", name)
    # Eliminar símbolos y espacios
    name = re.sub(r'[^\w\s]', '', name)
    # Reemplazar espacios con guiones bajos
    name = re.sub(r'\s+', '_', name)
    # Eliminar diacríticos (acentos) y convertir a minúsculas
    name = name.encode("ascii", "ignore").decode("ascii")
    return name.strip().lower()


def standardize_units(df):
    """
    Versión mejorada con nombres de columnas normalizados.
    """
    conversion_factors = {
        "dap_cm": ("dbh_in", 0.393701),  # Nombres normalizados
        "at_m": ("tht_ft", 3.28084),
        "at_defecto_m": ("defect_ht_ft", 3.28084),
        "alt_com_m": ("merch_ht_ft", 3.28084)
    }

    for metric_col, (imperial_col, factor) in conversion_factors.items():
        if metric_col in df.columns:
            if imperial_col not in df.columns:
                print(f"🔁 Convirtiendo {metric_col} a {imperial_col}")
                df[imperial_col] = pd.to_numeric(df[metric_col], errors="coerce") * factor
            else:
                print(f"✔️ {imperial_col} ya existe")

    return df


def clean_cruise_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Versión con debug mejorado.
    """
    # Paso 0: Debug antes de modificar
    #print("\n=== clean_cruise_dataframe() ===")
    #print("Columnas CRUDAS:", df.columns.tolist())
    #print("Ejemplo de fila cruda:", df.iloc[0].to_dict())  # 👈 ¡Nuevo!


    # Debug después de normalizar
    #print("\nColumnas NORMALIZADAS:", df.columns.tolist())
    #print("Ejemplo de fila normalizada:", df.iloc[0].to_dict())  # 👈 ¡Nuevo!

    # Eliminar filas vacías (solo si TODAS las columnas son NaN)
    df = df.dropna(how='all').copy()

    # Debug después de dropna
    #print("\nColumnas después de dropna:", df.columns.tolist())
    #print("Filas restantes:", len(df))

    return df