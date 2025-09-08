# core/region.py

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
    if not region:
        return None
    return COUNTRY_PREFIX.get(str(region).strip(), None)

# 👇 NUEVO: mapea prefijo → nombre canónico de país
PREFIX_TO_COUNTRY = {
    "US": "USA",
    "MX": "Mexico",
    "CR": "Costa Rica",
    "GT": "Guatemala",
}

def normalize_region_name(region: str) -> str | None:
    """
    Recibe 'USA', 'US', 'United States', 'México', etc. → devuelve
    'USA' | 'Mexico' | 'Costa Rica' | 'Guatemala' | None
    """
    p = get_prefix(region)
    return PREFIX_TO_COUNTRY.get(p)

# 👇 conveniente para usar directo sobre Series de pandas
def normalize_region_series(s):
    import pandas as _pd
    return _pd.Series(
        [normalize_region_name(x) for x in s],
        index=s.index,
        dtype="string"
    )

# --- NUEVO: derivar región desde contract_code ---
import re

def prefix_from_code(code) -> str | None:
    """Extrae prefijo 'US'/'MX'/'CR'/'GT' del contract_code."""
    if code is None:
        return None
    s = str(code).strip().upper()
    m = re.match(r'^([A-Z]{2,3})', s)
    if not m:
        return None
    p = m.group(1)
    if p == "USA":  # normaliza 3 letras a 2
        p = "US"
    return p if p in PREFIX_TO_COUNTRY else None

def region_from_code(code) -> str | None:
    """Devuelve nombre canónico a partir del contract_code."""
    p = prefix_from_code(code)
    return PREFIX_TO_COUNTRY.get(p)
