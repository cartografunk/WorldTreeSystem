# core/region.py

import re
from typing import Optional

# Prefijos válidos
VALID_PREFIXES = {"US", "MX", "CR", "GT"}

def get_prefix(region: str | None) -> Optional[str]:
    """
    Recibe variantes de nombre de país o prefijo y devuelve siempre
    'US' | 'MX' | 'CR' | 'GT' | None.
    """
    if not region:
        return None
    s = str(region).strip().upper()

    # Normaliza algunos alias comunes
    aliases = {
        "USA": "US",
        "UNITED STATES": "US",
        "MEXICO": "MX",
        "MÉXICO": "MX",
        "COSTA RICA": "CR",
        "GUATEMALA": "GT",
    }
    p = aliases.get(s, s[:2])
    return p if p in VALID_PREFIXES else None


def prefix_from_code(code: str | None) -> Optional[str]:
    """
    Extrae prefijo (US/MX/CR/GT) a partir del contract_code.
    Ej: 'MX0031' → 'MX', 'USA001' → 'US'.
    """
    if not code:
        return None
    s = str(code).strip().upper()
    m = re.match(r'^([A-Z]{2,3})', s)
    if not m:
        return None
    p = m.group(1)
    if p == "USA":  # normaliza 3 letras a 2
        p = "US"
    return p if p in VALID_PREFIXES else None


def region_from_code(code: str | None) -> Optional[str]:
    """
    Alias de prefix_from_code, para usar directo en joins/Series.
    """
    return prefix_from_code(code)
