#MasteDatabaseManagement/tools/db_helpers.py

from typing import Any, Dict, Iterable, Optional
from sqlalchemy import text
from core.db import get_engine

def run_tx(sql: str, params: Optional[Dict[str, Any]] = None) -> None:
    """Ejecuta una sentencia (o varias) en una transacción corta."""
    eng = get_engine()
    with eng.begin() as conn:
        conn.execute(text(sql), params or {})

def fetch_scalar(sql: str, params: Optional[Dict[str, Any]] = None) -> Any:
    """Devuelve un escalar (o None)."""
    eng = get_engine()
    with eng.begin() as conn:
        return conn.execute(text(sql), params or {}).scalar()

def exec_many(sql: str, records: Iterable[Dict[str, Any]]) -> int:
    """Ejecuta la misma sentencia para muchos registros."""
    eng = get_engine()
    with eng.begin() as conn:
        for r in records:
            conn.execute(text(sql), r)
    return len(list(records))
