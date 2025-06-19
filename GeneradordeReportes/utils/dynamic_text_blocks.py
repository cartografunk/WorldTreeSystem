#GeneradordeReportes/utils/dynamic_text_blocks

from core.libs import text
from GeneradordeReportes.utils.db     import get_engine
from GeneradordeReportes.utils.helpers import get_sql_column
from sqlalchemy.exc import ProgrammingError, OperationalError
from core.schema_helpers import get_column
from GeneradordeReportes.utils.helpers import get_inventory_table_name, get_region_language


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
        WHERE "Defect_id" IS NOT NULL AND "Defect_id" != 'None'
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


def fetch_dynamic_values(code: str, country: str, year: int):
    engine = get_engine()
    values = {}
    lang = get_region_language(country)
    table_name = get_inventory_table_name(country, year)

    # Usa get_column para resolver todos los nombres de columnas
    col_tht = get_column("tht_ft")       # altura total
    col_mht = get_column("merch_ht_ft")  # altura comercial
    col_dbh = get_column("dbh_in")
    col_alive = get_column("alive_tree")
    col_contract = get_column("contractcode")
    col_defect = get_column("Defect_id")

    # Usar estos nombres en los .format()
    with engine.connect() as conn:
        trans = conn.begin()
        for key, info in DYNAMIC_BLOCKS.items():
            try:
                if f"sql_{lang}" in info:
                    sql_raw = info[f"sql_{lang}"]
                else:
                    sql_raw = info["sql"]

                sql_filled = sql_raw.format(
                    code=code,
                    table=f"public.{table_name}",
                    contractcode=col_contract,
                    alive_tree=col_alive,
                    merch_ht=col_mht,
                    tht=col_tht,
                    dbh=col_dbh,
                )

                row = conn.execute(text(sql_filled)).scalar_one_or_none()
                if row is None:
                    print(f"⚠️ No se encontró valor para {key}")
                    continue
                values[key] = int(row) if key.startswith("count_") else row

            except (ProgrammingError, OperationalError, KeyError) as e:
                print(f"⚠️ Consulta fallida para '{key}': {e}")
                trans.rollback()
                trans = conn.begin()
                continue
        trans.commit()
    return values

def format_paragraphs(values, country):
    lang = get_region_language(country)
    paragraphs = []
    omit_keys = {"contractcode", "farmer_number", "planting_year", "contract_trees"}

    for key, val in values.items():
        if key in omit_keys:
            continue
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

