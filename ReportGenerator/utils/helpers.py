# ReportGenerator/utils/helpers.py
from core.libs import os, plt, inspect, re, NoSuchTableError, text
from core.schema import COLUMNS

def guardar_figura(path, fig, facecolor='white'):
    if not os.path.exists(path):
        fig.savefig(path, dpi=300, bbox_inches='tight', facecolor=facecolor)
        print(f"✅ Guardado: {path}")
    else:
        print(f"⚠️ Ya existe y no se sobreescribió: {path}")
    plt.close(fig)


def get_region_language(country_code: str = "CR") -> str:
    """
    Retorna 'es' para CR, GT, MX; 'en' para US; por defecto 'es'.
    """
    mapping = {
        "CR": "es",
        "GT": "es",
        "MX": "es",
        "US": "en",
    }
    return mapping.get(country_code.upper(), "es")

def get_inventory_table_name(country: str, year: int) -> str:
    return f"inventory_{country.lower()}_{year}"


def get_sql_column(key: str) -> str:
    match = next((col for col in COLUMNS if col["key"] == key), None)
    if match:
        return match["sql_name"]
    raise KeyError(f"Key '{key}' not found in schema.")




def normalize(s: str) -> str:
    # todo a minúsculas, solo alfanuméricos
    return re.sub(r'[^a-z0-9]', '', s.lower())

def resolve_column(engine, table_name, hint):
    """
    1) Inspecciona la tabla para obtener sus columnas reales.
    2) Construye un map dinámico { key_de_schema: nombre_real } probando cada alias.
    3) Devuelve nombre_real para la hint dada.
    4) Si no lo encuentra, lanza ValueError.
    """
    # 1) introspección de columnas reales
    inspector = inspect(engine)
    schema, tbl = (table_name.split('.',1) if '.' in table_name else (None, table_name))
    try:
        cols = inspector.get_columns(tbl, schema=schema)
    except NoSuchTableError:
        raise ValueError(f"Tabla {table_name!r} no existe en la base de datos")

    real_names = [c['name'] for c in cols]
    # normalizamos los nombres reales
    norm_real = { normalize(r): r for r in real_names }

    # 2) filtramos solo las definiciones de COLUMNS para esta tabla
    defs = [col for col in COLUMNS if col.get("table") == tbl]

    # 3) probamos cada alias de cada definición
    key_to_real = {}
    for col in defs:
        for alias in col.get("aliases", []):
            na = normalize(alias)
            if na in norm_real:
                # mapeamos la key del schema al nombre real de la BD
                key_to_real[col["key"].lower()] = norm_real[na]
                break

    # 4) normalizamos la hint y buscamos en el map
    nh = normalize(hint)
    # permitimos match por key o por alias dentro del mismo loop
    if nh in key_to_real:
        return key_to_real[nh]

    # 5) fallback puro: match hint contra columnas reales
    if nh in norm_real:
        return norm_real[nh]

    raise ValueError(f"No pude resolver '{hint}' en tabla {table_name!r}")

def tiene_datos_campo(engine, table, contractcode, campo):
    """True si existe al menos un valor NO nulo en el campo para el contrato dado."""
    from sqlalchemy import text
    sql = text(f'''
        SELECT COUNT(*) FROM public.{table}
        WHERE contractcode = :code AND "{campo}" IS NOT NULL
    ''')
    with engine.connect() as conn:
        count = conn.execute(sql, {"code": contractcode}).scalar_one()
    return count > 0