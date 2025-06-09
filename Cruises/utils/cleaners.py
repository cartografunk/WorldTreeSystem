#core/cleaners.py
from core.libs import pd

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

def remove_blank_rows(df: pd.DataFrame,
                      campos_clave: list[str] = None) -> pd.DataFrame:
    """
    Elimina filas completamente vacías en los campos clave (útiles).
    """
    campos_clave = campos_clave or ["plot", "tree_number", "Status"]
    campos_existentes = [c for c in campos_clave if c in df.columns]

    df = df.replace("", pd.NA)
    return df.dropna(subset=campos_existentes, how="all")
