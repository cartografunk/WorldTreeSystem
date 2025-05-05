from sqlalchemy import text
from utils.db     import get_engine
from utils.helpers import get_region_language

# Bloques dinámicos: traducciones y SQL
DYNAMIC_BLOCKS = {
    "avg_height_live": {
        "es": "Altura promedio de árboles vivos: {value:.2f} m",
        "en": "Average height of live trees: {value:.2f} m",
        "sql": '''
            SELECT AVG("Merch. HT (ft)") * 0.3048 AS value
            FROM public.inventory_cr_2025
            WHERE alive_tree = 1;
        '''
    },
    "count_over_2m": {
        "es": "Número de árboles con altura ≥ 2 m: {value:d}",
        "en": "Number of trees with height ≥ 2 m: {value:d}",
        "sql": '''
            SELECT COUNT(*) AS value
            FROM public.inventory_cr_2025
            WHERE "Merch. HT (ft)" * 0.3048 >= 2;
        '''
    },
    "avg_dbh": {
        "es": "Diámetro promedio (DBH): {value:.2f} cm",
        "en": "Average diameter (DBH): {value:.2f} cm",
        "sql": '''
            SELECT AVG("DBH (in)") * 2.54 AS value
            FROM public.inventory_cr_2025
            WHERE alive_tree = 1;
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
    }
}

def fetch_dynamic_values():
    engine = get_engine()
    values = {}
    with engine.connect() as conn:
        for key, info in DYNAMIC_BLOCKS.items():
            row = conn.execute(text(info["sql"])).scalar_one()
            values[key] = int(row) if key.startswith("count_") else float(row)
    return values

def format_paragraphs(values):
    lang = get_region_language()
    paragraphs = []
    for key, val in values.items():
        tmpl = DYNAMIC_BLOCKS[key][lang]
        paragraphs.append(tmpl.format(value=val))
    return paragraphs
