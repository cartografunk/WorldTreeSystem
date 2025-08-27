#core/region.py

COUNTRY_PREFIX = {
    "USA": "US",
    "United States": "US",
    "Mexico": "MX",
    "México": "MX",
    "Costa Rica": "CR",
    "CR": "CR",
    "Guatemala": "GT",
    "US": "US"
}

def get_prefix(region: str) -> str | None:
    """
    Normaliza un valor de Region (ej. 'USA', 'México') y devuelve
    el prefijo de Contract Code (ej. 'US', 'MX', 'CR', 'GT').
    """
    if not region:
        return None
    return COUNTRY_PREFIX.get(str(region).strip(), None)