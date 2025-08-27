#MasterDatabaseManagement\tools\minimal_parsers.py
from core.libs import pd


def _to_int(v):
    n = pd.to_numeric(v, errors="coerce")
    return int(n) if pd.notna(n) else None


def _to_float(v):
    n = pd.to_numeric(v, errors="coerce")
    return float(n) if pd.notna(n) else None


def _to_date(v):
    if v is None or str(v).strip() == "":
        return None
    t = pd.to_datetime(v, errors="coerce", dayfirst=True)
    return t.date() if pd.notna(t) else None


def _is_blank(v) -> bool:
    if v is None:
        return True
    s = str(v).replace("\u00A0", " ").strip()
    return s == ""
