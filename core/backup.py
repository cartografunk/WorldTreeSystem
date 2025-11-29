# core/backup.py
from __future__ import annotations
from typing import Iterable, List, Dict
from core.libs import datetime, Path, shutil
import core.backup_manager as backup_manager


def _ts() -> str:
    """Legacy timestamp function - kept for backup_excel compatibility."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


# ---------------- Excel ----------------
def backup_excel(path: str | Path) -> Path:
    """
    Copia el Excel a <nombre>_bkp_YYYYmmdd_HHMMSS.xlsx

    NOTE: This function handles file system backups and is NOT replaced
    by backup_manager (which handles database backups only).
    """
    src = Path(path)
    dst = src.with_name(f"{src.stem}_bkp_{_ts()}{src.suffix}")
    shutil.copyfile(src, dst)
    print(f"📄 Backup Excel: {dst}")
    return dst


# ---------------- Postgres ----------------
def backup_table(engine, table: str, schema: str = "masterdatabase", label: str = "bkp") -> str:
    """
    DEPRECATED: Use core.backup_manager.backup_table() instead.

    This function now wraps the centralized backup system.

    Creates a backup using the new backup_manager system.
    The 'label' parameter is ignored (kept for backward compatibility).

    Args:
        engine: SQLAlchemy engine
        table: Table name to backup
        schema: Source schema (default: "masterdatabase")
        label: DEPRECATED - ignored, kept for compatibility

    Returns:
        Fully qualified backup table name: backups.<backup_table>
    """
    if label != "bkp":
        print(f"⚠️  WARNING: The 'label' parameter is deprecated and will be ignored.")
        print(f"   All backups now use the centralized system in 'backups' schema.")

    # Use the new centralized backup system
    backup_name = backup_manager.backup_table(engine, table, schema=schema)

    # Return fully qualified name for backward compatibility
    return f"backups.{backup_name}"


def backup_tables(
        engine,
        tables: Iterable[str],
        schema: str = "masterdatabase",
        label: str = "bkp"
) -> List[str]:
    """
    DEPRECATED: Use core.backup_manager.backup_tables() instead.

    This function now wraps the centralized backup system.

    Args:
        engine: SQLAlchemy engine
        tables: Iterable of table names to backup
        schema: Source schema (default: "masterdatabase")
        label: DEPRECATED - ignored, kept for compatibility

    Returns:
        List of fully qualified backup table names
    """
    if label != "bkp":
        print(f"⚠️  WARNING: The 'label' parameter is deprecated and will be ignored.")

    # Use the new centralized backup system
    results = backup_manager.backup_tables(engine, list(tables), schema=schema)

    # Convert results to list of fully qualified names for backward compatibility
    created = []
    for table, result in results.items():
        if result["success"]:
            created.append(f"backups.{result['backup']}")
        else:
            print(f"⚠️  No se pudo respaldar {schema}.{table}: {result['error']}")

    return created