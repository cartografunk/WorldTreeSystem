from sqlalchemy import text
from GeneradordeReportes.utils.db     import get_engine
from GeneradordeReportes.utils.helpers import get_region_language

# Bloques dinámicos: traducciones y SQL
DYNAMIC_BLOCKS = {
    "avg_height_live": {
    "es": "Altura promedio de árboles vivos: {value:.2f} m",
    "en": "Average height of live trees: {value:.2f} m",
    "sql": '''
        SELECT AVG("Merch. HT (ft)") * 0.3048 AS value
        FROM public.inventory_cr_2025
        WHERE alive_tree = 1 AND "Merch. HT (ft)" BETWEEN 1 AND 100;
    '''
    },
    "count_over_2m": {
    "es": "Número de árboles con altura ≥ 2 m: {value:d}",
    "en": "Number of trees with height ≥ 2 m: {value:d}",
    "sql": '''
        SELECT COUNT(*) AS value
        FROM public.inventory_cr_2025
        WHERE "Merch. HT (ft)" BETWEEN 1 AND 100
          AND "Merch. HT (ft)" * 0.3048 >= 2;
    '''
    },
    "avg_dbh": {
    "es": "Diámetro promedio (DBH): {value:.2f} cm",
    "en": "Average diameter (DBH): {value:.2f} cm",
    "sql": '''
        SELECT AVG("DBH (in)") * 2.54 AS value
        FROM public.inventory_cr_2025
        WHERE alive_tree = 1
          AND "DBH (in)" BETWEEN 1 AND 50;
    '''
    },
    "count_defects": {
        "es": "Árboles con defecto registrado: {value:d}",
        "en": "Trees with recorded defect: {value:d}",
        "sql": '''
            SELECT COUNT(*) AS value
            FROM public.inventory_cr_2025
            WHERE cat_defect_id IS NOT NULL;
        '''
    },
    "contractcode": {
        "sql": "SELECT contract_code FROM masterdatabase.contract_tree_information WHERE contract_code = '{code}'",
        "es": "Contrato: {code}",
        "en": "Contract Code: {code}"
    },
    "farmer_number": {
        "sql": "SELECT farmer_number FROM masterdatabase.contract_tree_information WHERE contract_code = '{code}'",
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

def fetch_dynamic_values(code: str):
    engine = get_engine()
    values = {}

    with engine.connect() as conn:
        trans = conn.begin()
        for key, info in DYNAMIC_BLOCKS.items():
            sql_raw = info["sql"].format(code=code)
            try:
                row = conn.execute(text(sql_raw)).scalar_one_or_none()
                if row is None:
                    print(f"⚠️ No se encontró valor para {key}")
                    continue
                values[key] = int(row) if key.startswith("count_") else row
            except (ProgrammingError, OperationalError) as e:
                print(f"⚠️ Consulta fallida para '{key}': {e.orig}")
                trans.rollback()  # evitar que la conexión se bloquee
                trans = conn.begin()  # reiniciar la transacción para la siguiente
                continue
        trans.commit()

    return values




def format_paragraphs(values):
    lang = get_region_language()
    paragraphs = []
    for key, val in values.items():
        tmpl = DYNAMIC_BLOCKS[key][lang]
        try:
            # Si el template usa {value}, se reemplaza directo
            if "{value" in tmpl:
                paragraphs.append(tmpl.format(value=val))
            else:
                # Si usa {contractcode}, {farmer_number}, etc., se inyecta todo el dict
                paragraphs.append(tmpl.format(**values))
        except KeyError as e:
            print(f"⚠️ Placeholder faltante en '{key}': {e}")
            continue
    return paragraphs

