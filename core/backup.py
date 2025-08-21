# core/backup.py
from __future__ import annotations
from typing import Iterable, List
from core.libs import datetime, Path, shutil, text

def _ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")

# ---------------- Excel ----------------
def backup_excel(path: str | Path) -> Path:
    """Copia el Excel a <nombre>_bkp_YYYYmmdd_HHMMSS.xlsx"""
    src = Path(path)
    dst = src.with_name(f"{src.stem}_bkp_{_ts()}{src.suffix}")
    shutil.copyfile(src, dst)
    print(f"📝 Backup Excel: {dst}")
    return dst

# ---------------- Postgres ----------------
def backup_table(engine, table: str, schema: str = "masterdatabase", label: str = "bkp") -> str:
    """
    Crea una copia completa de la tabla: <schema>.<table>_<label>_<TS>
    Usa CREATE TABLE AS SELECT (incluye datos).
    Devuelve el nombre totalmente cualificado creado.
    """
    ts = _ts()
    bkp = f'{schema}."{table}_{label}_{ts}"'
    src = f'{schema}."{table}"'
    sql = text(f"CREATE TABLE {bkp} AS TABLE {src};")
    with engine.begin() as conn:
        conn.execute(sql)
    print(f"🛡️  Backup tabla: {bkp}")
    return bkp

def backup_tables(engine, tables: Iterable[str], schema: str = "masterdatabase", label: str = "bkp") -> List[str]:
    created = []
    for t in tables:
        try:
            created.append(backup_table(engine, t, schema=schema, label=label))
        except Exception as e:
            print(f"⚠️  No se pudo respaldar {schema}.{t}: {e}")
    return created
