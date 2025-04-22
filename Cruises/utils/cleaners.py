# utils/cleaners.py
from utils.libs import pd, unicodedata, re
from utils.column_mapper import COLUMN_LOOKUP


def get_column(df, logical_name):
    """
    Versión con debug detallado.
    """
    posibles = COLUMN_LOOKUP.get(logical_name, [logical_name])

    # Debug 1: Mostrar nombres originales y normalizados
    #print(f"\n🔍 Búsqueda detallada para: {logical_name}")
    #print("=== Aliases originales ===")
    #print(posibles)
    #print("=== Aliases normalizados ===")
    #print([clean_column_name(alias) for alias in posibles])
    #print("=== Columnas reales en el DF ===")
    #print(df.columns.tolist())
    #print("=== Columnas normalizadas del DF ===")
    #print([clean_column_name(col) for col in df.columns])

    # Buscar coincidencias
    for alias in posibles:
        if alias in df.columns:  # Primero buscar sin normalizar
            #print(f"✅ Coincidencia exacta: '{alias}'")
            return alias

    # Si no hay coincidencia exacta, buscar normalizado
    posibles_normalizados = [clean_column_name(alias) for alias in posibles]
    df_columns_normalized = [clean_column_name(col) for col in df.columns]

    for alias_norm, alias_orig in zip(posibles_normalizados, posibles):
        if alias_norm in df_columns_normalized:
            idx = df_columns_normalized.index(alias_norm)
            #print(f"✅ Coincidencia normalizada: '{alias_orig}' → '{df.columns[idx]}'")
            return df.columns[idx]

    raise KeyError(f"❌ Columna '{logical_name}' no encontrada. Aliases probados: {posibles}")


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