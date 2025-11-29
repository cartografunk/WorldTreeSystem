#!/usr/bin/env python3
"""
Validation script to check backup system setup before running tests.

Run this before executing tests to ensure the database is properly configured.
"""

from sqlalchemy import create_engine, text, inspect
import sys


def get_engine():
    return create_engine(
        "postgresql+psycopg2://postgres:pauwlonia@localhost:5432/helloworldtree"
    )


def check_schema_exists(engine, schema_name):
    """Check if a schema exists."""
    inspector = inspect(engine)
    exists = schema_name in inspector.get_schema_names()
    return exists


def check_table_exists(engine, schema, table):
    """Check if a table exists."""
    inspector = inspect(engine)
    exists = table in inspector.get_table_names(schema=schema)
    return exists


def get_table_count(engine, schema):
    """Get count of tables in schema."""
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT COUNT(*) 
            FROM pg_tables 
            WHERE schemaname = :schema
              AND tablename != 'metadata'
              AND tablename != 'backups_log'
        """), {"schema": schema})
        return result.scalar()


def check_metadata_table_structure(engine):
    """Verify metadata table has correct columns."""
    expected_columns = {
        'id', 'source_schema', 'source_table', 'backup_schema',
        'backup_table', 'created_at', 'row_count', 'notes'
    }

    inspector = inspect(engine)
    columns = inspector.get_columns('metadata', schema='backups')
    actual_columns = {col['name'] for col in columns}

    return expected_columns == actual_columns


def check_permissions(engine):
    """Check if user has required permissions."""
    try:
        with engine.begin() as conn:
            # Test create table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS backups.test_permissions_check (id INT)
            """))
            # Test drop table
            conn.execute(text("""
                DROP TABLE IF EXISTS backups.test_permissions_check
            """))
        return True
    except Exception as e:
        return False, str(e)


def validate_setup():
    """Run all validation checks."""
    print("=" * 70)
    print("BACKUP SYSTEM VALIDATION")
    print("=" * 70)
    print()

    engine = get_engine()
    all_passed = True

    # Check 1: backups schema exists
    print("✓ Checking 'backups' schema...")
    if check_schema_exists(engine, 'backups'):
        print("  ✅ Schema 'backups' exists")
    else:
        print("  ❌ Schema 'backups' NOT FOUND")
        print("     → Run: migrations/001_backup_system_migration.sql")
        all_passed = False

    # Check 2: masterdatabase schema exists
    print("✓ Checking 'masterdatabase' schema...")
    if check_schema_exists(engine, 'masterdatabase'):
        print("  ✅ Schema 'masterdatabase' exists")
    else:
        print("  ❌ Schema 'masterdatabase' NOT FOUND")
        all_passed = False

    # Check 3: metadata table exists
    print("✓ Checking 'backups.metadata' table...")
    if check_table_exists(engine, 'backups', 'metadata'):
        print("  ✅ Table 'backups.metadata' exists")

        # Check structure
        if check_metadata_table_structure(engine):
            print("  ✅ Metadata table structure is correct")
        else:
            print("  ⚠️  Metadata table structure might be incorrect")
    else:
        print("  ❌ Table 'backups.metadata' NOT FOUND")
        print("     → Run: migrations/001_backup_system_migration.sql")
        all_passed = False

    # Check 4: Permissions
    print("✓ Checking database permissions...")
    perm_result = check_permissions(engine)
    if perm_result is True:
        print("  ✅ User has required permissions (CREATE, DROP)")
    else:
        print("  ❌ Permission check failed")
        if isinstance(perm_result, tuple):
            print(f"     Error: {perm_result[1]}")
        all_passed = False

    # Check 5: Existing backups
    print("✓ Checking existing backups...")
    backup_count = get_table_count(engine, 'backups')
    print(f"  ℹ️  Found {backup_count} existing backup tables in 'backups' schema")

    if check_schema_exists(engine, 'masterdatabase_backups'):
        control_count = get_table_count(engine, 'masterdatabase_backups')
        print(f"  ℹ️  Found {control_count} backup_control tables in 'masterdatabase_backups' schema")

    # Check 6: Metadata entries
    print("✓ Checking metadata entries...")
    with engine.connect() as conn:
        metadata_count = conn.execute(text("""
            SELECT COUNT(*) FROM backups.metadata
        """)).scalar()
        print(f"  ℹ️  Found {metadata_count} entries in metadata table")

    # Check 7: Look for orphaned backups in masterdatabase
    print("✓ Checking for orphaned backups in masterdatabase...")
    with engine.connect() as conn:
        orphaned = conn.execute(text("""
            SELECT tablename 
            FROM pg_tables 
            WHERE schemaname = 'masterdatabase'
              AND (
                  tablename LIKE '%_bkp_%' OR
                  tablename LIKE '%backup%' OR
                  tablename LIKE '%_old' OR
                  tablename LIKE '%__bak%'
              )
        """)).fetchall()

        if orphaned:
            print(f"  ⚠️  Found {len(orphaned)} orphaned backup tables in masterdatabase:")
            for row in orphaned[:5]:  # Show first 5
                print(f"     - {row[0]}")
            if len(orphaned) > 5:
                print(f"     ... and {len(orphaned) - 5} more")
            print("     → Run: migrations/002_cleanup_existing_backups.sql")
        else:
            print("  ✅ No orphaned backups in masterdatabase")

    print()
    print("=" * 70)

    if all_passed:
        print("✅ ALL CHECKS PASSED - System is ready for use")
        print()
        print("You can now:")
        print("  1. Run tests: pytest tests/test_backup_manager.py -v")
        print("  2. Use backup system in your code")
        return 0
    else:
        print("❌ SOME CHECKS FAILED - Please fix issues before using the system")
        print()
        print("Common fixes:")
        print("  1. Run: migrations/001_backup_system_migration.sql")
        print("  2. Run: migrations/002_cleanup_existing_backups.sql")
        print("  3. Check database permissions")
        return 1


if __name__ == "__main__":
    sys.exit(validate_setup())