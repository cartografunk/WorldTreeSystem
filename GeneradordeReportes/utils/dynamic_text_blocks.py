from sqlalchemy import text
from GeneradordeReportes.utils.db     import get_engine
from GeneradordeReportes.utils.helpers import get_region_language, get_inventory_table_name
from GeneradordeReportes.utils.helpers import get_sql_column

# Bloques dinámicos: traducciones y SQL
DYNAMIC_BLOCKS = {
    "avg_height_live": {
    "es": "Altura promedio de árboles vivos: {value:.2f} m",
    "en": "Average height of live trees: {value:.2f} ft",
    "sql": '''
        SELECT AVG("{tht}") AS value
        FROM {table}
        WHERE "{contractcode}" = '{code}' AND "{alive_tree}" = 1
          AND "{dbh}" BETWEEN 1 AND 50;
    '''
    },
    "count_over_2m": {
    "es": "Número de árboles con altura ≥ 2 m: {value:d}",
    "en": "Number of trees with height ≥ 2 m: {value:d}",
    "sql": '''
        SELECT COUNT(*) AS value
        FROM {table}
        WHERE "{contractcode}" = '{code}'
          AND "{merch_ht}" BETWEEN 1 AND 100
          AND "{merch_ht}" >= 2;
    '''
    },
    "avg_dbh": {
    "es": "Diámetro promedio (DBH): {value:.2f} cm",
    "en": "Average diameter (DBH): {value:.2f} in",
    "sql": '''
        SELECT AVG("{tht}") AS value
        FROM {table}
        WHERE "{contractcode}" = '{code}' AND "{alive_tree}" = 1
          AND "{dbh}" BETWEEN 1 AND 50;
    ''',
    },
    "count_defects": {
    "es": "Árboles con defecto registrado: {value:d}",
    "en": "Trees with recorded defect: {value:d}",
    "sql": '''
        SELECT COUNT(*) AS value
        FROM {table}
        WHERE "{contractcode}" = '{code}' AND "{defect_id}" IS NOT NULL;
    '''
    },
    "contractcode": {
        "sql": "SELECT contract_code FROM masterdatabase.contract_tree_information WHERE contract_code = '{code}'",
        "es": "Contrato: {code}",
        "en": "Contract Code: {code}"
    },
    "farmer_number": {
        "sql": "SELECT farmer_number FROM masterdatabase.contract_farmer_information WHERE contract_code = '{code}'",
        "es": "Código del productor: {farmer_number}",
        "en": "Farmer Code: {farmer_number}"
    },
    "planting_year": {
        "sql": "SELECT planting_year FROM masterdatabase.contract_tree_information WHERE contract_code = '{code}'",
        "es": "Año de plantación: {planting_year}",
        "en": "Planting Year: {planting_year}"
    },
    "contract_trees": {
        "sql": "SELECT trees_contract FROM masterdatabase.contract_tree_information WHERE contract_code = '{code}'",
        "es": "Total de árboles contratados: {contract_trees}",
        "en": "Contracted trees: {contract_trees}"
    },
}

from sqlalchemy.exc import ProgrammingError, OperationalError

def fetch_dynamic_values(code: str, country: str = "cr", year: int = 2025):
    engine = get_engine()
    values = {}
    lang = get_region_language(country)
    table_name = get_inventory_table_name(country, year)

    with engine.connect() as conn:
        trans = conn.begin()
        for key, info in DYNAMIC_BLOCKS.items():
            try:
                # Determinar la plantilla SQL según idioma (si aplica)
                if f"sql_{lang}" in info:
                    sql_raw = info[f"sql_{lang}"]
                else:
                    sql_raw = info["sql"]

                # Formatear con columnas reales
                sql_filled = sql_raw.format(
                    code=code,
                    table=f"public.{table_name}",
                    contractcode=get_sql_column("contractcode"),
                    alive_tree=get_sql_column("alive_tree"),
                    merch_ht=get_sql_column("merch_ht_ft"),
                    dbh=get_sql_column("dbh_in"),
                    defect_id=get_sql_column("defect_id")
                )

                row = conn.execute(text(sql_filled)).scalar_one_or_none()
                if row is None:
                    print(f"⚠️ No se encontró valor para {key}")
                    continue
                values[key] = int(row) if key.startswith("count_") else row

            except (ProgrammingError, OperationalError) as e:
                print(f"⚠️ Consulta fallida para '{key}': {e.orig}")
                trans.rollback()
                trans = conn.begin()
                continue
        trans.commit()

    return values




def format_paragraphs(values):
    lang = get_region_language()
    paragraphs = []

    # Claves que ya se usaron en la tabla introductoria
    omit_keys = {"contractcode", "farmer_number", "planting_year", "contract_trees"}

    for key, val in values.items():
        if key in omit_keys:
            continue  # Omitir datos repetidos de la introducción

        tmpl = DYNAMIC_BLOCKS[key][lang]
        try:
            if "{value" in tmpl:
                paragraphs.append(tmpl.format(value=val))
            else:
                paragraphs.append(tmpl.format(**values))
        except KeyError as e:
            print(f"⚠️ Placeholder faltante en '{key}': {e}")
            continue

    return paragraphs


