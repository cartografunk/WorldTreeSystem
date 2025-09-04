# core/backup_control.py
from datetime import datetime
from typing import Iterable, Optional, Sequence, Tuple
from sqlalchemy import text
from core.libs import Path, pd
from core.db import get_engine
from core.paths import DATABASE_EXPORTS_DIR
from tqdm import tqdm
import re
import os
import glob

BACKUP_SCHEMA = "masterdatabase_backups"
BACKUP_LOG_TABLE = f"{BACKUP_SCHEMA}.backups_log"

DDL_SCHEMA = f"""
CREATE SCHEMA IF NOT EXISTS {BACKUP_SCHEMA};
"""

DDL_LOG = f"""
CREATE TABLE IF NOT EXISTS {BACKUP_LOG_TABLE} (
  id            BIGSERIAL PRIMARY KEY,
  ts            TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_schema TEXT        NOT NULL,
  source_table  TEXT        NOT NULL,
  backup_schema TEXT        NOT NULL,
  backup_table  TEXT        NOT NULL,
  tag           TEXT        NOT NULL,
  rows_copied   BIGINT      NOT NULL
);
"""

def _safe_ident(x: str) -> str:
    # Sanea a snake_case simple para nombres de tabla de backup
    x = re.sub(r"[^\w]+", "_", x).strip("_").lower()
    return x

def ensure_backup_infra(engine):
    with engine.begin() as conn:
        conn.execute(text(DDL_SCHEMA))
        conn.execute(text(DDL_LOG))

def _backup_table_name(source_table: str, tag: str) -> str:
    # Un solo nombre estable por tabla+tag; siempre se reemplaza
    return f"{_safe_ident(source_table)}__pre_{_safe_ident(tag)}"

def drop_existing_backup(engine, backup_table: str):
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS {BACKUP_SCHEMA}."{backup_table}"'))

def create_backup_from_source(engine, source_schema: str, source_table: str, backup_table: str) -> int:
    # Copia física de datos (sin índices/constraints)
    with engine.begin() as conn:
        conn.execute(text(
            f'CREATE TABLE {BACKUP_SCHEMA}."{backup_table}" AS '
            f'SELECT * FROM "{source_schema}"."{source_table}"'
        ))
        # Contar filas copiadas
        rows = conn.execute(text(
            f'SELECT COUNT(*) FROM {BACKUP_SCHEMA}."{backup_table}"'
        )).scalar_one()
    return int(rows)

def log_backup(engine, source_schema: str, source_table: str, backup_table: str, tag: str, rows: int):
    with engine.begin() as conn:
        conn.execute(text(f"""
            INSERT INTO {BACKUP_LOG_TABLE}
            (source_schema, source_table, backup_schema, backup_table, tag, rows_copied)
            VALUES (:ss, :st, :bs, :bt, :tag, :rows)
        """), {
            "ss": source_schema,
            "st": source_table,
            "bs": BACKUP_SCHEMA,
            "bt": backup_table,
            "tag": tag,
            "rows": rows
        })

def backup_once(
    tables: Sequence[Tuple[str, str]],
    tag: str,
    engine=None
) -> pd.DataFrame:
    """
    Crea un *único* backup por tabla y tag, reemplazando el anterior.
    tables: lista de (source_schema, source_table)
    tag:    identificador de tarea, p.ej. 'changelog_activation' o 'new_contracts_input'
    """
    engine = engine or get_engine()
    ensure_backup_infra(engine)

    records = []
    for ss, st in tqdm(tables, desc=f"Backing up ({tag})", unit="table"):
        backup_table = _backup_table_name(st, tag)
        drop_existing_backup(engine, backup_table)
        rows = create_backup_from_source(engine, ss, st, backup_table)
        log_backup(engine, ss, st, backup_table, tag, rows)
        records.append({
            "source_schema": ss, "source_table": st,
            "backup_schema": BACKUP_SCHEMA, "backup_table": backup_table,
            "tag": tag, "rows_copied": rows
        })
    return pd.DataFrame.from_records(records)

# ===================== Excel rotation (opcional) =====================

def rotate_excel_backup_single_latest(tag: str, pattern_prefix: str, ext: str = "xlsx") -> Optional[Path]:
    """
    Deja un solo Excel por tag: borra previos con mismo prefijo/tag y crea uno nuevo.
    pattern_prefix: prefijo del nombre de archivo, p.ej. 'changelog_bkp'
    """
    export_dir = Path(DATABASE_EXPORTS_DIR)
    export_dir.mkdir(parents=True, exist_ok=True)

    # Borra los existentes del mismo tag/prefijo
    pat = os.path.join(str(export_dir), f"{pattern_prefix}_{_safe_ident(tag)}_*.{ext}")
    for f in glob.glob(pat):
        try:
            os.remove(f)
        except Exception:
            pass

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = export_dir / f"{pattern_prefix}_{_safe_ident(tag)}_{ts}.{ext}"
    return out

def save_excel_single_latest(df_map: dict, tag: str, pattern_prefix: str) -> Path:
    """
    df_map: {"SheetName": dataframe}
    Crea un único Excel por tag y prefijo (sobrescribe lógicos previos).
    """
    out = rotate_excel_backup_single_latest(tag, pattern_prefix, ext="xlsx")
    with pd.ExcelWriter(out, engine="xlsxwriter") as xw:
        for sheet, dfx in df_map.items():
            dfx.to_excel(xw, sheet_name=sheet, index=False)
    return out
