# tests/test_backup_manager.py
"""
Comprehensive test suite for the backup_manager module.

Tests cover:
- Backup creation and validation
- Backup pruning and retention policy
- Metadata tracking
- Error handling and edge cases
- Transactional safety
- Idempotency
"""

import pytest
from datetime import datetime
from sqlalchemy import create_engine, text, inspect
import pandas as pd
import time

import core.backup_manager as bm
from core.backup_manager import BackupError


@pytest.fixture(scope="function")
def test_engine():
    """Creates a test database engine with cleanup."""
    engine = create_engine(
        "postgresql+psycopg2://postgres:pauwlonia@localhost:5432/helloworldtree"
    )

    yield engine

    # Cleanup after test - FIXED: DO $$ instead of DO $
    with engine.begin() as conn:
        # Drop test tables in masterdatabase
        conn.execute(text("""
            DO $$ 
            DECLARE r RECORD;
            BEGIN
                FOR r IN (SELECT tablename FROM pg_tables 
                         WHERE schemaname = 'masterdatabase' 
                         AND tablename LIKE 'test_%') 
                LOOP
                    EXECUTE 'DROP TABLE IF EXISTS masterdatabase.' || quote_ident(r.tablename) || ' CASCADE';
                END LOOP;
            END $$;
        """))

        # Drop test backups
        conn.execute(text("""
            DO $$ 
            DECLARE r RECORD;
            BEGIN
                FOR r IN (SELECT tablename FROM pg_tables 
                         WHERE schemaname = 'backups' 
                         AND tablename LIKE 'test_%') 
                LOOP
                    EXECUTE 'DROP TABLE IF EXISTS backups.' || quote_ident(r.tablename) || ' CASCADE';
                END LOOP;
            END $$;
        """))

        # Clean up test entries from metadata
        conn.execute(text("""
            DELETE FROM backups.metadata 
            WHERE source_table LIKE 'test_%' 
               OR backup_table LIKE 'test_%'
        """))

    engine.dispose()


@pytest.fixture
def sample_table(test_engine):
    """Creates a sample test table."""
    table_name = "test_sample_data"

    # Create sample table
    with test_engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS masterdatabase"))
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS masterdatabase.{table_name} (
                id SERIAL PRIMARY KEY,
                name TEXT,
                value INTEGER,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """))

        # Insert sample data
        conn.execute(text(f"""
            INSERT INTO masterdatabase.{table_name} (name, value)
            VALUES 
                ('Alice', 100),
                ('Bob', 200),
                ('Charlie', 300)
        """))

    yield table_name

    # Cleanup handled by test_engine fixture


class TestBackupCreation:
    """Tests for create_backup function."""

    def test_create_backup_success(self, test_engine, sample_table):
        """Test successful backup creation."""
        backup_name = bm.create_backup(test_engine, sample_table)

        # Verify backup exists
        inspector = inspect(test_engine)
        assert backup_name in inspector.get_table_names(schema="backups")

        # Verify backup has correct data
        with test_engine.connect() as conn:
            original_count = conn.execute(
                text(f"SELECT COUNT(*) FROM masterdatabase.{sample_table}")
            ).scalar()
            backup_count = conn.execute(
                text(f"SELECT COUNT(*) FROM backups.{backup_name}")
            ).scalar()

            assert backup_count == original_count == 3

    def test_create_backup_naming_pattern(self, test_engine, sample_table):
        """Test backup follows correct naming pattern."""
        backup_name = bm.create_backup(test_engine, sample_table)

        # Should match: <table>_YYYYMMDD_HHMMSS
        assert backup_name.startswith(f"{sample_table}_")

        timestamp_part = backup_name[len(sample_table) + 1:]
        assert len(timestamp_part) >= 15  # YYYYMMDD_HHMMSS = 15 chars minimum

    def test_create_backup_nonexistent_table(self, test_engine):
        """Test backup of non-existent table raises error."""
        with pytest.raises(BackupError, match="does not exist"):
            bm.create_backup(test_engine, "nonexistent_table")

    def test_create_backup_metadata_recorded(self, test_engine, sample_table):
        """Test that metadata is properly recorded."""
        backup_name = bm.create_backup(test_engine, sample_table)

        # Check metadata table
        with test_engine.connect() as conn:
            result = conn.execute(text("""
                SELECT source_table, backup_table, row_count 
                FROM backups.metadata 
                WHERE backup_table = :backup_name
            """), {"backup_name": backup_name}).mappings().one()

            assert result["source_table"] == sample_table
            assert result["backup_table"] == backup_name
            assert result["row_count"] == 3

    def test_create_backup_collision_handling(self, test_engine, sample_table):
        """Test that name collisions are handled correctly."""
        # Create first backup
        backup1 = bm.create_backup(test_engine, sample_table)

        # Create second backup immediately (may have same second)
        backup2 = bm.create_backup(test_engine, sample_table)

        # Names should be different
        assert backup1 != backup2

        # Both should exist
        inspector = inspect(test_engine)
        tables = inspector.get_table_names(schema="backups")
        assert backup1 in tables
        assert backup2 in tables


class TestBackupRetrieval:
    """Tests for get_existing_backups function."""

    def test_get_backups_empty(self, test_engine):
        """Test retrieval when no backups exist."""
        backups = bm.get_existing_backups(test_engine, "nonexistent_table")
        assert backups == []

    def test_get_backups_single(self, test_engine, sample_table):
        """Test retrieval of single backup."""
        backup_name = bm.create_backup(test_engine, sample_table)

        backups = bm.get_existing_backups(test_engine, sample_table)

        assert len(backups) == 1
        assert backups[0][0] == backup_name
        assert isinstance(backups[0][1], datetime)

    def test_get_backups_multiple_sorted(self, test_engine, sample_table):
        """Test that multiple backups are sorted newest first."""
        # Create multiple backups with small delays
        backup1 = bm.create_backup(test_engine, sample_table)
        time.sleep(0.1)
        backup2 = bm.create_backup(test_engine, sample_table)
        time.sleep(0.1)
        backup3 = bm.create_backup(test_engine, sample_table)

        backups = bm.get_existing_backups(test_engine, sample_table)

        assert len(backups) == 3
        # Should be sorted newest first
        assert backups[0][0] == backup3
        assert backups[1][0] == backup2
        assert backups[2][0] == backup1

    def test_get_backups_filters_other_tables(self, test_engine, sample_table):
        """Test that backups for other tables are not included."""
        # Create backup for sample_table
        bm.create_backup(test_engine, sample_table)

        # Create another test table and backup
        with test_engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE masterdatabase.test_other_table (id INT)
            """))
        bm.create_backup(test_engine, "test_other_table")

        # Get backups for sample_table only
        backups = bm.get_existing_backups(test_engine, sample_table)

        # Should only have 1 backup (for sample_table)
        assert len(backups) == 1
        assert backups[0][0].startswith(sample_table)


class TestBackupPruning:
    """Tests for prune_backups function."""

    def test_prune_no_backups(self, test_engine):
        """Test pruning when no backups exist."""
        deleted = bm.prune_backups(test_engine, "nonexistent_table")
        assert deleted == []

    def test_prune_single_backup(self, test_engine, sample_table):
        """Test that single backup is not deleted."""
        bm.create_backup(test_engine, sample_table)

        deleted = bm.prune_backups(test_engine, sample_table)

        assert deleted == []
        # Backup should still exist
        backups = bm.get_existing_backups(test_engine, sample_table)
        assert len(backups) == 1

    def test_prune_multiple_backups(self, test_engine, sample_table):
        """Test that only newest backup is kept."""
        # Create 3 backups
        backup1 = bm.create_backup(test_engine, sample_table)
        time.sleep(0.1)
        backup2 = bm.create_backup(test_engine, sample_table)
        time.sleep(0.1)
        backup3 = bm.create_backup(test_engine, sample_table)

        # Prune
        deleted = bm.prune_backups(test_engine, sample_table)

        # Should delete 2 older backups
        assert len(deleted) == 2
        assert backup1 in deleted
        assert backup2 in deleted
        assert backup3 not in deleted

        # Only newest should remain
        backups = bm.get_existing_backups(test_engine, sample_table)
        assert len(backups) == 1
        assert backups[0][0] == backup3


class TestBackupTableFunction:
    """Tests for high-level backup_table function."""

    def test_backup_table_creates_and_prunes(self, test_engine, sample_table):
        """Test that backup_table creates backup and prunes old ones."""
        # Create initial backup
        backup1 = bm.backup_table(test_engine, sample_table)

        # Create second backup
        time.sleep(0.1)
        backup2 = bm.backup_table(test_engine, sample_table)

        # Only the latest should exist
        backups = bm.get_existing_backups(test_engine, sample_table)
        assert len(backups) == 1
        assert backups[0][0] == backup2

        # First backup should be deleted
        inspector = inspect(test_engine)
        tables = inspector.get_table_names(schema="backups")
        assert backup2 in tables
        assert backup1 not in tables

    def test_backup_table_idempotent(self, test_engine, sample_table):
        """Test that multiple calls to backup_table work correctly."""
        for i in range(3):
            backup_name = bm.backup_table(test_engine, sample_table)
            assert backup_name is not None
            time.sleep(0.1)

        # Should have exactly 1 backup (the latest)
        backups = bm.get_existing_backups(test_engine, sample_table)
        assert len(backups) == 1


class TestBatchBackup:
    """Tests for backup_tables function."""

    def test_backup_tables_all_success(self, test_engine):
        """Test batch backup with all successful operations."""
        # Create multiple test tables
        tables = []
        with test_engine.begin() as conn:
            for i in range(3):
                table_name = f"test_batch_{i}"
                conn.execute(text(f"""
                    CREATE TABLE masterdatabase.{table_name} (id INT)
                """))
                conn.execute(text(f"""
                    INSERT INTO masterdatabase.{table_name} VALUES ({i})
                """))
                tables.append(table_name)

        # Batch backup
        results = bm.backup_tables(test_engine, tables)

        # All should succeed
        assert len(results) == 3
        for table in tables:
            assert results[table]["success"] is True
            assert results[table]["backup"] is not None
            assert results[table]["error"] is None

    def test_backup_tables_partial_failure(self, test_engine, sample_table):
        """Test batch backup with some failures."""
        tables = [sample_table, "nonexistent_table", sample_table]

        results = bm.backup_tables(test_engine, tables)

        # Check mixed results
        assert results[sample_table]["success"] is True
        assert results["nonexistent_table"]["success"] is False
        assert results["nonexistent_table"]["error"] is not None

    def test_backup_tables_continues_after_failure(self, test_engine, sample_table):
        """Test that batch continues processing after individual failure."""
        # Create another valid table
        with test_engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE masterdatabase.test_batch_valid (id INT)
            """))

        tables = [sample_table, "nonexistent_table", "test_batch_valid"]

        results = bm.backup_tables(test_engine, tables)

        # First and third should succeed despite middle failure
        assert results[sample_table]["success"] is True
        assert results["test_batch_valid"]["success"] is True
        assert results["nonexistent_table"]["success"] is False


class TestPandasIntegration:
    """Tests for to_sql_with_backup wrapper."""

    def test_to_sql_append_no_backup(self, test_engine):
        """Test that append mode doesn't trigger backup."""
        df = pd.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})

        # This should not create a backup
        bm.to_sql_with_backup(
            df,
            "test_pandas_append",
            test_engine,
            schema="masterdatabase",
            if_exists="append",
            index=False
        )

        # No backups should exist
        backups = bm.get_existing_backups(test_engine, "test_pandas_append")
        assert len(backups) == 0

    def test_to_sql_replace_creates_backup(self, test_engine):
        """Test that replace mode creates backup."""
        df = pd.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})

        # Create initial table
        df.to_sql(
            "test_pandas_replace",
            test_engine,
            schema="masterdatabase",
            if_exists="replace",
            index=False
        )

        # Replace with backup
        df_new = pd.DataFrame({"id": [4, 5], "value": [40, 50]})
        bm.to_sql_with_backup(
            df_new,
            "test_pandas_replace",
            test_engine,
            schema="masterdatabase",
            if_exists="replace",
            index=False
        )

        # Should have created a backup
        backups = bm.get_existing_backups(test_engine, "test_pandas_replace")
        assert len(backups) == 1


class TestTransactionalSafety:
    """Tests for transactional behavior and error recovery."""

    def test_backup_failure_preserves_existing(self, test_engine, sample_table):
        """Test that if new backup fails, existing backup is preserved."""
        # Create first backup
        backup1 = bm.create_backup(test_engine, sample_table)

        # Verify it exists
        backups_before = bm.get_existing_backups(test_engine, sample_table)
        assert len(backups_before) == 1

        # Try to backup non-existent table (should fail)
        with pytest.raises(BackupError):
            bm.backup_table(test_engine, "nonexistent_table")

        # Original backup should still exist
        backups_after = bm.get_existing_backups(test_engine, sample_table)
        assert len(backups_after) == 1
        assert backups_after[0][0] == backup1

    def test_metadata_consistency(self, test_engine, sample_table):
        """Test that metadata is consistent with actual backups."""
        # Create multiple backups
        for i in range(3):
            bm.backup_table(test_engine, sample_table)
            time.sleep(0.1)

        # Get actual backups
        backups = bm.get_existing_backups(test_engine, sample_table)

        # Get metadata entries
        with test_engine.connect() as conn:
            metadata_count = conn.execute(text("""
                SELECT COUNT(*) FROM backups.metadata
                WHERE source_table = :table
            """), {"table": sample_table}).scalar()

        # Should have 3 metadata entries (one for each created)
        # but only 1 actual backup (due to pruning)
        assert len(backups) == 1
        assert metadata_count == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])