# GeneradordeReportes/utils/helpers.py

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
