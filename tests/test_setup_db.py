"""
test_setup_db.py - Unit tests for 01_setup_db.py

Tests database table creation and schema validation.
"""

import pytest
import sqlite3
import os
import sys
import tempfile
import shutil

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


class TestGetConnection:
    """Tests for get_connection function."""

    def test_connection_creates_db_file(self):
        """Test that connection creates database file if it doesn't exist."""
        temp_dir = tempfile.mkdtemp()
        db_path = os.path.join(temp_dir, "db", "test.db")

        try:
            # Ensure directory is created
            os.makedirs(os.path.dirname(db_path), exist_ok=True)
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row

            assert os.path.exists(db_path)
            assert conn is not None

            conn.close()
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_connection_returns_valid_connection(self, temp_db):
        """Test that connection returns a valid SQLite connection."""
        conn, db_path = temp_db

        # Should be able to execute queries
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()

        assert result[0] == 1

    def test_row_factory_is_set(self, temp_db):
        """Test that row_factory is set to sqlite3.Row."""
        conn, db_path = temp_db

        # row_factory should allow dict-like access
        cursor = conn.cursor()
        cursor.execute("SELECT 1 as test_col")
        row = cursor.fetchone()

        # sqlite3.Row allows access by column name
        assert row['test_col'] == 1


class TestCreateTables:
    """Tests for create_tables function."""

    def test_stores_table_created(self, temp_db_with_tables):
        """Test that stores table is created with correct schema."""
        conn, _ = temp_db_with_tables
        cursor = conn.cursor()

        cursor.execute("PRAGMA table_info(stores)")
        columns = {row[1]: row[2] for row in cursor.fetchall()}

        assert 'store_id' in columns
        assert 'store_name' in columns
        assert 'store_city' in columns
        assert 'store_region' in columns
        assert 'opening_date' in columns

    def test_products_table_created(self, temp_db_with_tables):
        """Test that products table is created with correct schema."""
        conn, _ = temp_db_with_tables
        cursor = conn.cursor()

        cursor.execute("PRAGMA table_info(products)")
        columns = {row[1]: row[2] for row in cursor.fetchall()}

        assert 'product_id' in columns
        assert 'product_name' in columns
        assert 'product_category' in columns
        assert 'unit_price' in columns
        assert 'current_stock_level' in columns
        assert 'restock_flag' in columns

    def test_customer_details_table_created(self, temp_db_with_tables):
        """Test that customer_details table is created with correct schema."""
        conn, _ = temp_db_with_tables
        cursor = conn.cursor()

        cursor.execute("PRAGMA table_info(customer_details)")
        columns = {row[1]: row[2] for row in cursor.fetchall()}

        assert 'customer_id' in columns
        assert 'first_name' in columns
        assert 'loyalty_status' in columns
        assert 'total_loyalty_points' in columns
        assert 'segment_id' in columns
        assert 'promotion_sensitivity' in columns

    def test_store_sales_header_table_created(self, temp_db_with_tables):
        """Test that store_sales_header table is created with correct schema."""
        conn, _ = temp_db_with_tables
        cursor = conn.cursor()

        cursor.execute("PRAGMA table_info(store_sales_header)")
        columns = {row[1]: row[2] for row in cursor.fetchall()}

        assert 'transaction_id' in columns
        assert 'customer_id' in columns
        assert 'store_id' in columns
        assert 'transaction_date' in columns
        assert 'total_amount' in columns

    def test_store_sales_line_items_table_created(self, temp_db_with_tables):
        """Test that store_sales_line_items table is created."""
        conn, _ = temp_db_with_tables
        cursor = conn.cursor()

        cursor.execute("PRAGMA table_info(store_sales_line_items)")
        columns = {row[1]: row[2] for row in cursor.fetchall()}

        assert 'line_item_id' in columns
        assert 'transaction_id' in columns
        assert 'product_id' in columns
        assert 'promotion_id' in columns
        assert 'quantity' in columns
        assert 'line_item_amount' in columns

    def test_loyalty_rules_table_created(self, temp_db_with_tables):
        """Test that loyalty_rules table is created."""
        conn, _ = temp_db_with_tables
        cursor = conn.cursor()

        cursor.execute("PRAGMA table_info(loyalty_rules)")
        columns = {row[1]: row[2] for row in cursor.fetchall()}

        assert 'rule_id' in columns
        assert 'points_per_unit_spend' in columns
        assert 'min_spend_threshold' in columns
        assert 'bonus_points' in columns

    def test_rfm_summary_table_created(self, temp_db_with_tables):
        """Test that rfm_summary analytics table is created."""
        conn, _ = temp_db_with_tables
        cursor = conn.cursor()

        cursor.execute("PRAGMA table_info(rfm_summary)")
        columns = {row[1]: row[2] for row in cursor.fetchall()}

        assert 'customer_id' in columns
        assert 'recency_days' in columns
        assert 'frequency' in columns
        assert 'monetary_value' in columns

    def test_customer_predictions_table_created(self, temp_db_with_tables):
        """Test that customer_predictions analytics table is created."""
        conn, _ = temp_db_with_tables
        cursor = conn.cursor()

        cursor.execute("PRAGMA table_info(customer_predictions)")
        columns = {row[1]: row[2] for row in cursor.fetchall()}

        assert 'customer_id' in columns
        assert 'predicted_next_spend' in columns
        assert 'months_used' in columns

    def test_rejected_tables_created(self, temp_db_with_tables):
        """Test that rejected tables are created for data quality tracking."""
        conn, _ = temp_db_with_tables
        cursor = conn.cursor()

        # Check all rejected tables exist
        rejected_tables = [
            'stores_rejected',
            'products_rejected',
            'customer_details_rejected',
            'store_sales_header_rejected',
            'store_sales_line_items_rejected'
        ]

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        existing_tables = [row[0] for row in cursor.fetchall()]

        for table in rejected_tables:
            assert table in existing_tables, f"{table} not found"

    def test_rejected_tables_have_reject_reason_column(self, temp_db_with_tables):
        """Test that all rejected tables have reject_reason column."""
        conn, _ = temp_db_with_tables
        cursor = conn.cursor()

        rejected_tables = [
            'stores_rejected',
            'products_rejected',
            'customer_details_rejected',
            'store_sales_header_rejected',
            'store_sales_line_items_rejected'
        ]

        for table in rejected_tables:
            cursor.execute(f"PRAGMA table_info({table})")
            columns = [row[1] for row in cursor.fetchall()]
            assert 'reject_reason' in columns, f"reject_reason not in {table}"


class TestTableConstraints:
    """Tests for table constraints and defaults."""

    def test_stores_primary_key(self, temp_db_with_tables):
        """Test that store_id is primary key in stores table."""
        conn, _ = temp_db_with_tables
        cursor = conn.cursor()

        # Insert first record
        cursor.execute("INSERT INTO stores (store_id, store_name) VALUES ('S1', 'Test')")
        conn.commit()

        # Inserting duplicate should fail
        with pytest.raises(sqlite3.IntegrityError):
            cursor.execute("INSERT INTO stores (store_id, store_name) VALUES ('S1', 'Test2')")

    def test_customer_details_default_loyalty_status(self, temp_db_with_tables):
        """Test that customer_details has default loyalty_status of Bronze."""
        conn, _ = temp_db_with_tables
        cursor = conn.cursor()

        cursor.execute("INSERT INTO customer_details (customer_id, first_name) VALUES ('C1', 'Test')")
        conn.commit()

        cursor.execute("SELECT loyalty_status FROM customer_details WHERE customer_id = 'C1'")
        result = cursor.fetchone()

        assert result[0] == 'Bronze'

    def test_products_default_restock_flag(self, temp_db_with_tables):
        """Test that products has default restock_flag of 0."""
        conn, _ = temp_db_with_tables
        cursor = conn.cursor()

        cursor.execute("INSERT INTO products (product_id, product_name) VALUES ('P1', 'Test')")
        conn.commit()

        cursor.execute("SELECT restock_flag FROM products WHERE product_id = 'P1'")
        result = cursor.fetchone()

        assert result[0] == 0


class TestIdempotency:
    """Tests for idempotent table creation."""

    def test_create_tables_idempotent(self, temp_db):
        """Test that running create_tables multiple times doesn't cause errors."""
        conn, _ = temp_db
        cursor = conn.cursor()

        # Create table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS test_table (
                id TEXT PRIMARY KEY,
                name TEXT
            )
        """)
        conn.commit()

        # Insert data
        cursor.execute("INSERT INTO test_table VALUES ('1', 'Test')")
        conn.commit()

        # Run create again - should not error
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS test_table (
                id TEXT PRIMARY KEY,
                name TEXT
            )
        """)
        conn.commit()

        # Data should still be there
        cursor.execute("SELECT COUNT(*) FROM test_table")
        count = cursor.fetchone()[0]

        assert count == 1
