# core/backup_manager.py
"""
Centralized Backup Manager for WorldTreeSystem.

This module provides a robust, deterministic backup system that:
- Stores all backups in the 'backups' schema
- Uses naming pattern: <table>_YYYYMMDD_HHMMSS[_mmm][_N]
- Keeps only one backup per table (newest)
- Provides metadata tracking for all backup operations
"""

from datetime import datetime, timezone
from typing import List, Tuple, Dict, Optional
from sqlalchemy import text, inspect
from sqlalchemy.engine import Engine
import logging

# Configure logging
logger = logging.getLogger(__name__)


class BackupError(Exception):
    """Custom exception for backup operations."""
    pass


def _ensure_backup_schema(engine: Engine, backup_schema: str = "backups") -> None:
    """
    Ensures the backup schema exists. Creates it if necessary.

    Args:
        engine: SQLAlchemy engine
        backup_schema: Name of the backup schema
    """
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {backup_schema}"))
        logger.debug(f"Schema {backup_schema} ensured")


def _ensure_metadata_table(engine: Engine, backup_schema: str = "backups") -> None:
    """
    Creates the metadata table to track all backup operations.

    Table structure:
        - id: SERIAL PRIMARY KEY
        - source_schema: TEXT
        - source_table: TEXT
        - backup_schema: TEXT
        - backup_table: TEXT
        - created_at: TIMESTAMP WITH TIME ZONE
        - row_count: BIGINT
        - notes: TEXT
    """
    _ensure_backup_schema(engine, backup_schema)

    sql = f"""
    CREATE TABLE IF NOT EXISTS {backup_schema}.metadata (
        id SERIAL PRIMARY KEY,
        source_schema TEXT NOT NULL,
        source_table TEXT NOT NULL,
        backup_schema TEXT NOT NULL,
        backup_table TEXT NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        row_count BIGINT,
        notes TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_metadata_source 
        ON {backup_schema}.metadata(source_schema, source_table);
    CREATE INDEX IF NOT EXISTS idx_metadata_backup 
        ON {backup_schema}.metadata(backup_schema, backup_table);
    CREATE INDEX IF NOT EXISTS idx_metadata_created 
        ON {backup_schema}.metadata(created_at DESC);
    """

    with engine.begin() as conn:
        conn.execute(text(sql))
        logger.debug(f"Metadata table ensured in {backup_schema}")


def _table_exists(engine: Engine, table: str, schema: str) -> bool:
    """
    Checks if a table exists in the given schema.

    Args:
        engine: SQLAlchemy engine
        table: Table name
        schema: Schema name

    Returns:
        True if table exists, False otherwise
    """
    inspector = inspect(engine)
    return table in inspector.get_table_names(schema=schema)


def _get_row_count(engine: Engine, table: str, schema: str) -> int:
    """
    Gets the row count for a table.

    Args:
        engine: SQLAlchemy engine
        table: Table name
        schema: Schema name

    Returns:
        Number of rows in the table
    """
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{table}"))
        return result.scalar()


def _generate_backup_name(table: str, existing_backups: List[str]) -> str:
    """
    Generates a unique backup name using UTC timestamp.
    Handles collisions by adding millisecond suffix and incremental counter.

    Pattern:
        <table>_YYYYMMDD_HHMMSS
        <table>_YYYYMMDD_HHMMSS_mmm
        <table>_YYYYMMDD_HHMMSS_mmm_2

    Args:
        table: Original table name
        existing_backups: List of existing backup names to check for collisions

    Returns:
        Unique backup table name
    """
    now = datetime.now(timezone.utc)
    base_name = f"{table}_{now.strftime('%Y%m%d_%H%M%S')}"

    # Check if base name exists
    if base_name not in existing_backups:
        return base_name

    # Add milliseconds
    ms_name = f"{base_name}_{now.strftime('%f')[:3]}"
    if ms_name not in existing_backups:
        return ms_name

    # Add incremental counter
    counter = 2
    while True:
        incremental_name = f"{ms_name}_{counter}"
        if incremental_name not in existing_backups:
            return incremental_name
        counter += 1
        if counter > 1000:  # Safety limit
            raise BackupError(f"Unable to generate unique backup name for {table} after 1000 attempts")


def _insert_metadata(
        engine: Engine,
        source_schema: str,
        source_table: str,
        backup_schema: str,
        backup_table: str,
        row_count: int,
        notes: Optional[str] = None
) -> None:
    """
    Inserts a metadata record for the backup operation.

    Args:
        engine: SQLAlchemy engine
        source_schema: Source schema name
        source_table: Source table name
        backup_schema: Backup schema name
        backup_table: Backup table name
        row_count: Number of rows backed up
        notes: Optional notes about the backup
    """
    sql = f"""
    INSERT INTO {backup_schema}.metadata 
        (source_schema, source_table, backup_schema, backup_table, row_count, notes)
    VALUES 
        (:source_schema, :source_table, :backup_schema, :backup_table, :row_count, :notes)
    """

    with engine.begin() as conn:
        conn.execute(text(sql), {
            "source_schema": source_schema,
            "source_table": source_table,
            "backup_schema": backup_schema,
            "backup_table": backup_table,
            "row_count": row_count,
            "notes": notes
        })


def create_backup(
        engine: Engine,
        table: str,
        source_schema: str = "masterdatabase",
        backup_schema: str = "backups"
) -> str:
    """
    Creates a backup of the specified table.

    This function:
    1. Validates that the source table exists
    2. Ensures the backup schema exists
    3. Generates a unique backup name using UTC timestamp
    4. Creates the backup table (structure + data)
    5. Records metadata about the backup

    Args:
        engine: SQLAlchemy engine
        table: Name of the table to backup
        source_schema: Schema containing the source table (default: "masterdatabase")
        backup_schema: Schema where backup will be stored (default: "backups")

    Returns:
        Name of the created backup table (without schema)

    Raises:
        BackupError: If source table doesn't exist or backup creation fails
    """
    # Validate source table exists
    if not _table_exists(engine, table, source_schema):
        raise BackupError(f"Source table {source_schema}.{table} does not exist")

    # Ensure backup infrastructure exists
    _ensure_backup_schema(engine, backup_schema)
    _ensure_metadata_table(engine, backup_schema)

    # Get existing backups to avoid name collision
    existing_backups = [name for name, _ in get_existing_backups(engine, table, backup_schema)]

    # Generate unique backup name
    backup_table = _generate_backup_name(table, existing_backups)

    try:
        # Create backup table
        with engine.begin() as conn:
            conn.execute(text(
                f"CREATE TABLE {backup_schema}.{backup_table} AS TABLE {source_schema}.{table}"
            ))

        # Get row count
        row_count = _get_row_count(engine, backup_table, backup_schema)

        # Insert metadata
        _insert_metadata(
            engine,
            source_schema,
            table,
            backup_schema,
            backup_table,
            row_count,
            notes="Automatic backup via backup_manager"
        )

        logger.info(f"✅ Backup created: {backup_schema}.{backup_table} ({row_count} rows)")
        return backup_table

    except Exception as e:
        logger.error(f"❌ Failed to create backup for {source_schema}.{table}: {e}")
        raise BackupError(f"Failed to create backup: {e}") from e


def get_existing_backups(
        engine: Engine,
        table: str,
        backup_schema: str = "backups"
) -> List[Tuple[str, datetime]]:
    """
    Gets all existing backups for a table, sorted by timestamp (newest first).

    Matches patterns:
    - <table>_YYYYMMDD_HHMMSS
    - <table>_YYYYMMDD_HHMMSS_mmm (with milliseconds)
    - <table>_YYYYMMDD_HHMMSS_mmm_N (with counter)

    Args:
        engine: SQLAlchemy engine
        table: Original table name to find backups for
        backup_schema: Schema containing backups (default: "backups")

    Returns:
        List of tuples (backup_name, timestamp) sorted newest first.
        Returns empty list if no backups exist or schema doesn't exist.
    """
    # Check if backup schema exists
    inspector = inspect(engine)
    if backup_schema not in inspector.get_schema_names():
        return []

    # Get all tables in backup schema
    all_tables = inspector.get_table_names(schema=backup_schema)

    # Filter tables matching pattern and extract timestamps
    backups = []
    prefix = f"{table}_"

    for backup_name in all_tables:
        if not backup_name.startswith(prefix):
            continue

        # Extract timestamp part
        timestamp_part = backup_name[len(prefix):]

        # Parse timestamp - supports base format and suffixes
        try:
            # Pattern: YYYYMMDD_HHMMSS[_mmm][_N]
            # Extract base timestamp (first 15 chars: YYYYMMDD_HHMMSS)
            if len(timestamp_part) >= 15:
                date_str = timestamp_part[:15]
                dt = datetime.strptime(date_str, "%Y%m%d_%H%M%S")

                # If there's a millisecond suffix, add it to sort properly
                if len(timestamp_part) > 15 and timestamp_part[15] == '_':
                    # Extract milliseconds if present
                    rest = timestamp_part[16:]  # After the underscore
                    parts = rest.split('_')

                    if parts[0].isdigit() and len(parts[0]) == 3:
                        # Has milliseconds suffix
                        ms = int(parts[0])
                        # Add microseconds to datetime for proper sorting
                        dt = dt.replace(microsecond=ms * 1000)

                backups.append((backup_name, dt))
        except ValueError:
            # Not a valid backup timestamp pattern, skip
            continue

    # Sort by timestamp, newest first
    backups.sort(key=lambda x: x[1], reverse=True)

    return backups

def prune_backups(
        engine: Engine,
        table: str,
        backup_schema: str = "backups"
) -> List[str]:
    """
    Prunes old backups, keeping only the newest one.

    Args:
        engine: SQLAlchemy engine
        table: Original table name
        backup_schema: Schema containing backups (default: "backups")

    Returns:
        List of deleted backup table names
    """
    existing_backups = get_existing_backups(engine, table, backup_schema)

    if len(existing_backups) <= 1:
        # Nothing to prune (0 or 1 backup)
        return []

    # Keep the first (newest), delete the rest
    to_delete = [name for name, _ in existing_backups[1:]]

    deleted = []
    with engine.begin() as conn:
        for backup_name in to_delete:
            try:
                conn.execute(text(f"DROP TABLE IF EXISTS {backup_schema}.{backup_name}"))
                deleted.append(backup_name)
                logger.info(f"🗑️  Pruned old backup: {backup_schema}.{backup_name}")
            except Exception as e:
                logger.warning(f"⚠️  Failed to prune {backup_schema}.{backup_name}: {e}")

    return deleted


def backup_table(
        engine: Engine,
        table: str,
        schema: str = "masterdatabase"
) -> str:
    """
    High-level function to backup a table with automatic pruning.

    This function:
    1. Creates a new backup
    2. Prunes old backups (keeps only the newest)

    Args:
        engine: SQLAlchemy engine
        table: Name of the table to backup
        schema: Schema containing the table (default: "masterdatabase")

    Returns:
        Name of the backup table created (without schema)

    Raises:
        BackupError: If backup creation fails
    """
    # Create new backup first
    backup_name = create_backup(engine, table, source_schema=schema)

    # Then prune old backups
    deleted = prune_backups(engine, table)
    if deleted:
        logger.info(f"🧹 Pruned {len(deleted)} old backup(s) for {table}")

    return backup_name


def backup_tables(
        engine: Engine,
        tables: List[str],
        schema: str = "masterdatabase"
) -> Dict[str, dict]:
    """
    Backs up multiple tables with best-effort approach.

    Continues processing all tables even if individual backups fail.

    Args:
        engine: SQLAlchemy engine
        tables: List of table names to backup
        schema: Schema containing the tables (default: "masterdatabase")

    Returns:
        Dictionary mapping table names to results:
        {
            "table1": {"success": True, "backup": "table1_20250527_143022", "error": None},
            "table2": {"success": False, "backup": None, "error": "Table does not exist"}
        }
    """
    results = {}

    for table in tables:
        try:
            backup_name = backup_table(engine, table, schema=schema)
            results[table] = {
                "success": True,
                "backup": backup_name,
                "error": None
            }
        except Exception as e:
            logger.error(f"❌ Failed to backup {schema}.{table}: {e}")
            results[table] = {
                "success": False,
                "backup": None,
                "error": str(e)
            }

    # Summary
    success_count = sum(1 for r in results.values() if r["success"])
    total_count = len(tables)
    logger.info(f"📊 Batch backup completed: {success_count}/{total_count} successful")

    return results


# Utility function for pandas DataFrame.to_sql() wrapper
def to_sql_with_backup(
        df,
        table: str,
        engine: Engine,
        schema: str = "masterdatabase",
        if_exists: str = "fail",
        **kwargs
) -> None:
    """
    Wrapper for pandas DataFrame.to_sql() that creates backups before replace operations.

    Args:
        df: pandas DataFrame to write
        table: Target table name
        engine: SQLAlchemy engine
        schema: Target schema (default: "masterdatabase")
        if_exists: Behavior if table exists: 'fail', 'replace', 'append' (default: 'fail')
        **kwargs: Additional arguments passed to df.to_sql()

    Raises:
        BackupError: If backup fails (only for if_exists='replace')
    """
    # Only backup if we're replacing
    if if_exists == "replace" and _table_exists(engine, table, schema):
        logger.info(f"🛡️  Creating backup before to_sql(if_exists='replace') for {schema}.{table}")
        backup_table(engine, table, schema=schema)

    # Call the original to_sql
    df.to_sql(name=table, con=engine, schema=schema, if_exists=if_exists, **kwargs)