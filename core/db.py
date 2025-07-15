# core/db.py
from core.libs import create_engine, pd, datetime, text


def get_engine():
    """
    Crea y devuelve un engine de SQLAlchemy conectado a la base de datos.
    """
    engine = create_engine(
        "postgresql+psycopg2://postgres:pauwlonia@localhost:5432/helloworldtree"
    )
    print("💻 Conectado a la base de datos helloworldtree")
    return engine


def get_table_names(country_code: str = "cr", year: str = "2025", schema: str = "public") -> dict:
    """
    Devuelve un diccionario con las tablas clave de inventario para un país y año específicos.

    Args:
        country_code: Código del país (ej. 'cr', 'us', 'gt').
        year: Año de inventario (ej. '2025').
        schema: Esquema de la base de datos (por defecto 'public').

    Returns:
        Dict con claves descriptivas y nombres de tabla completos.
    """
    suffix = f"inventory_{country_code}_{year}"
    return {
        "Catálogo Contratos": f"{schema}.cat_{suffix}",
        "Inventario Detalle": f"{schema}.{suffix}"
    }


def inspect_tables(
    engine=None,
    table_dict: dict = None,
    country_code: str = "cr",
    year: str = "2025",
    schema: str = "public"
) -> None:
    """
    Inspecciona las tablas pasadas en table_dict (o las por defecto) y despliega
    primeros registros, info de columnas y estadísticas descriptivas.

    Args:
        engine: Engine de SQLAlchemy. Si es None, lo crea con get_engine().
        table_dict: Diccionario nombre->tabla. Si es None, usa get_table_names().
        country_code, year, schema: Parámetros para get_table_names() si table_dict es None.
    """
    if engine is None:
        engine = get_engine()
    if table_dict is None:
        table_dict = get_table_names(country_code, year, schema)

    for nombre_tabla, sql_tabla in table_dict.items():
        print(f"\n=== {nombre_tabla} ({sql_tabla}) ===")
        try:
            df = pd.read_sql(f"SELECT * FROM {sql_tabla} LIMIT 5", engine)
            print("\n→ Primeros registros:\n", df.to_string(index=False))
            print("\n→ Info de columnas:")
            df.info(verbose=True)
            print("\n→ Estadísticas descriptivas:\n", df.describe(include='all'))
        except Exception as e:
            print(f"⚠️ Error consultando {sql_tabla}: {e}")

def backup_table(table, schema="masterdatabase"):
    """
    Crea un respaldo rápido de la tabla SQL como backup con fecha y hora.
    Si la tabla no existe, ignora el error.
    """
    engine = get_engine()
    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_table = f"{schema}.{table}_bkp_{now}"
    try:
        with engine.connect() as conn:
            conn.execute(text(f"CREATE TABLE {backup_table} AS TABLE {schema}.{table}"))
        print(f"🛡️  Backup creado: {backup_table}")
    except Exception as e:
        print(f"⚠️  No se pudo respaldar {schema}.{table}: {e}")