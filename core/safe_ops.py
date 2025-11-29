# core/safe_ops.py
"""
Safe Database Operations Module

Provides safe wrappers for dangerous database operations:
- safe_drop_table: Drops table only after confirming backup exists
- safe_create_table_as: Creates table from SELECT with validation
- to_sql_with_backup: Pandas to_sql wrapper with automatic backups

All operations integrate with the backup_manager to ensure data safety.
"""

import logging
from typing import Optional
from sqlalchemy import text, inspect
from sqlalchemy.engine import Engine
import pandas as pd

from core.backup_manager import backup_table, get_existing_backups, BackupError

logger = logging.getLogger(__name__)


class SafeOpsError(Exception):
    """Custom exception for safe operations failures."""
    pass


def safe_drop_table(
        engine: Engine,
        table: str,
        schema: str = "masterdatabase",
        require_backup: bool = True
) -> None:
    """
    Safely drop a table after confirming backup exists.

    Args:
        engine: SQLAlchemy engine
        table: Table name to drop
        schema: Schema name (default: masterdatabase)
        require_backup: Whether to require backup before dropping (default: True)
                       Set to False for dropping backups themselves

    Raises:
        SafeOpsError: If backup doesn't exist or drop fails
    """
    inspector = inspect(engine)

    # Check if table exists
    if not inspector.has_table(table, schema=schema):
        logger.warning(f"Table {schema}.{table} does not exist, nothing to drop")
        return

    # Require backup check (unless explicitly disabled)
    if require_backup and schema == "masterdatabase":
        backups = get_existing_backups(engine, table)

        if not backups:
            raise SafeOpsError(
                f"Cannot drop {schema}.{table}: No backup exists. "
                f"Create backup first with backup_table(engine, '{table}')"
            )

        logger.info(f"✅ Backup verified for {table}: {backups[0][0]}")

    # Execute drop
    try:
        with engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS {schema}."{table}" CASCADE'))

        logger.info(f"🗑️  Dropped table: {schema}.{table}")

    except Exception as e:
        raise SafeOpsError(f"Failed to drop {schema}.{table}: {e}") from e


def safe_create_table_as(
        engine: Engine,
        schema: str,
        table: str,
        select_sql: str
) -> int:
    """
    Safely create a table from a SELECT statement.

    Args:
        engine: SQLAlchemy engine
        schema: Target schema
        table: Target table name
        select_sql: Complete SELECT statement (without CREATE TABLE part)

    Returns:
        Number of rows inserted

    Raises:
        SafeOpsError: If creation fails
    """
    # Validate inputs
    if not select_sql.strip().upper().startswith('SELECT'):
        raise SafeOpsError(
            "select_sql must start with SELECT. "
            f"Got: {select_sql[:50]}..."
        )

    # Check if table already exists
    inspector = inspect(engine)
    if inspector.has_table(table, schema=schema):
        raise SafeOpsError(
            f"Table {schema}.{table} already exists. "
            f"Drop it first with safe_drop_table() or use a different name."
        )

    try:
        # Create table
        create_sql = f'CREATE TABLE {schema}."{table}" AS\n{select_sql}'

        with engine.begin() as conn:
            conn.execute(text(create_sql))

            # Get row count
            result = conn.execute(text(f'SELECT COUNT(*) FROM {schema}."{table}"'))
            row_count = result.scalar()

        logger.info(f"✅ Created table: {schema}.{table} ({row_count} rows)")
        return row_count

    except Exception as e:
        raise SafeOpsError(
            f"Failed to create {schema}.{table}: {e}"
        ) from e


def to_sql_with_backup(
        df: pd.DataFrame,
        engine: Engine,
        table: str,
        schema: str = "masterdatabase",
        if_exists: str = "fail",
        **kwargs
) -> None:
    """
    Wrapper for pandas DataFrame.to_sql() that creates backups before replace operations.

    This function provides the same interface as pandas to_sql but adds automatic
    backup creation when replacing tables in the masterdatabase schema.

    Args:
        df: pandas DataFrame to write
        engine: SQLAlchemy engine
        table: Target table name
        schema: Target schema (default: masterdatabase)
        if_exists: Behavior if table exists: 'fail', 'replace', 'append' (default: fail)
        **kwargs: Additional arguments passed to df.to_sql()

    Raises:
        SafeOpsError: If backup fails (only for if_exists='replace')

    Examples:
        >>> to_sql_with_backup(
        ...     df,
        ...     engine,
        ...     'my_table',
        ...     schema='masterdatabase',
        ...     if_exists='replace',
        ...     index=False
        ... )
    """
    # Only backup if we're replacing a table in masterdatabase
    if if_exists == "replace" and schema == "masterdatabase":
        inspector = inspect(engine)

        # Check if table exists
        if inspector.has_table(table, schema=schema):
            try:
                logger.info(f"🛡️  Creating backup before to_sql(if_exists='replace') for {schema}.{table}")
                backup_table(engine, table, schema=schema)
            except BackupError as e:
                raise SafeOpsError(
                    f"Cannot replace {schema}.{table}: Backup failed. {e}"
                ) from e

    # Execute to_sql
    try:
        df.to_sql(
            name=table,
            con=engine,
            schema=schema,
            if_exists=if_exists,
            **kwargs
        )

        logger.info(f"✅ Data written to {schema}.{table} ({len(df)} rows)")

    except Exception as e:
        raise SafeOpsError(
            f"Failed to write data to {schema}.{table}: {e}"
        ) from e


def validate_safe_operations_available(engine: Engine) -> dict:
    """
    Validates that all safe operations infrastructure is available.

    Returns:
        Dict with validation results:
        {
            'backup_schema_exists': bool,
            'metadata_table_exists': bool,
            'can_create_backup': bool,
            'errors': [str]
        }
    """
    results = {
        'backup_schema_exists': False,
        'metadata_table_exists': False,
        'can_create_backup': False,
        'errors': []
    }

    try:
        inspector = inspect(engine)

        # Check backup schema
        schemas = inspector.get_schema_names()
        results['backup_schema_exists'] = 'backups' in schemas

        if not results['backup_schema_exists']:
            results['errors'].append("Backup schema 'backups' does not exist")

        # Check metadata table
        if results['backup_schema_exists']:
            tables = inspector.get_table_names(schema='backups')
            results['metadata_table_exists'] = 'metadata' in tables

            if not results['metadata_table_exists']:
                results['errors'].append("Metadata table 'backups.metadata' does not exist")

        # Test backup creation (dry run)
        if results['backup_schema_exists'] and results['metadata_table_exists']:
            results['can_create_backup'] = True
        else:
            results['errors'].append("Cannot create backups: infrastructure incomplete")

    except Exception as e:
        results['errors'].append(f"Validation error: {e}")

    return results


# Convenience function for checking if safe ops are ready
def ensure_safe_ops_ready(engine: Engine) -> None:
    """
    Ensures safe operations infrastructure is ready, raises if not.

    Raises:
        SafeOpsError: If infrastructure is not ready
    """
    validation = validate_safe_operations_available(engine)

    if not validation['can_create_backup']:
        error_msg = "Safe operations not ready:\n" + "\n".join(
            f"  - {err}" for err in validation['errors']
        )
        raise SafeOpsError(error_msg)

    logger.info("✅ Safe operations infrastructure validated")