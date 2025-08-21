#core/units.py

# En algún archivo de configuración común (utils/units.py por ejemplo)
UNITS_BLOCK = {
    "dbh": {
        "es": {"unit": "cm", "factor": 1, "label": "cm"},
        "en": {"unit": "in", "factor": 1/2.54, "label": "in"},  # convierte cm → in
    },
    "height": {
        "es": {"unit": "m", "factor": 1, "label": "m"},
        "en": {"unit": "ft", "factor": 3.28084, "label": "ft"},  # convierte m → ft
    }
}
