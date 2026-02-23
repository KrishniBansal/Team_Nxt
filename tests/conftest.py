"""
conftest.py - Shared pytest fixtures for RetailPulse unit tests

This module provides common fixtures used across test modules including:
- Temporary database connections
- Sample test data
- Test configuration
"""

import pytest
import sqlite3
import pandas as pd
import os
import tempfile
import shutil
from datetime import datetime


@pytest.fixture(scope="function")
def temp_db():
    """
    Create a temporary SQLite database for testing.

    Yields:
        tuple: (connection, db_path)
    """
    # Create a temporary directory
    temp_dir = tempfile.mkdtemp()
    db_path = os.path.join(temp_dir, "test_retail.db")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    yield conn, db_path

    # Cleanup
    conn.close()
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture(scope="function")
def temp_db_with_tables(temp_db):
    """
    Create a temporary database with all tables created.

    Yields:
        tuple: (connection, db_path)
    """
    conn, db_path = temp_db
    cursor = conn.cursor()

    # Create all required tables
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stores (
            store_id       TEXT PRIMARY KEY,
            store_name     TEXT NOT NULL,
            store_city     TEXT,
            store_region   TEXT,
            opening_date   TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            product_id          TEXT PRIMARY KEY,
            product_name        TEXT NOT NULL,
            product_category    TEXT,
            unit_price          REAL,
            current_stock_level INTEGER,
            restock_flag        INTEGER DEFAULT 0
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customer_details (
            customer_id            TEXT PRIMARY KEY,
            first_name             TEXT,
            email                  TEXT,
            loyalty_status         TEXT DEFAULT 'Bronze',
            total_loyalty_points   INTEGER DEFAULT 0,
            last_purchase_date     TEXT,
            segment_id             TEXT,
            customer_phone         TEXT,
            customer_since         TEXT,
            promotion_sensitivity  TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS store_sales_header (
            transaction_id   TEXT PRIMARY KEY,
            customer_id      TEXT,
            store_id         TEXT,
            transaction_date TEXT,
            total_amount     REAL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS store_sales_line_items (
            line_item_id      TEXT PRIMARY KEY,
            transaction_id    TEXT,
            product_id        TEXT,
            promotion_id      TEXT,
            quantity          INTEGER,
            line_item_amount  REAL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS promotion_details (
            promotion_id           TEXT PRIMARY KEY,
            promotion_name         TEXT,
            start_date             TEXT,
            end_date               TEXT,
            discount_percentage    REAL,
            applicable_category    TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS loyalty_rules (
            rule_id               TEXT PRIMARY KEY,
            rule_name             TEXT,
            points_per_unit_spend REAL,
            min_spend_threshold   REAL,
            bonus_points          INTEGER,
            start_date            TEXT,
            end_date              TEXT
        )
    """)

    # Create rejected tables
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stores_rejected (
            store_id TEXT, store_name TEXT, store_city TEXT,
            store_region TEXT, opening_date TEXT, reject_reason TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products_rejected (
            product_id TEXT, product_name TEXT, product_category TEXT,
            unit_price REAL, current_stock_level INTEGER,
            restock_flag INTEGER, reject_reason TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customer_details_rejected (
            customer_id TEXT, first_name TEXT, email TEXT,
            loyalty_status TEXT, total_loyalty_points INTEGER,
            last_purchase_date TEXT, segment_id TEXT, customer_phone TEXT,
            customer_since TEXT, promotion_sensitivity TEXT, reject_reason TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS store_sales_header_rejected (
            transaction_id TEXT, customer_id TEXT, store_id TEXT,
            transaction_date TEXT, total_amount REAL, reject_reason TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS store_sales_line_items_rejected (
            line_item_id TEXT, transaction_id TEXT, product_id TEXT,
            promotion_id TEXT, quantity INTEGER, line_item_amount REAL,
            reject_reason TEXT
        )
    """)

    # Analytics tables
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS rfm_summary (
            customer_id    TEXT PRIMARY KEY,
            recency_days   INTEGER,
            frequency      INTEGER,
            monetary_value REAL,
            calculated_at  TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customer_predictions (
            customer_id           TEXT PRIMARY KEY,
            predicted_next_spend  REAL,
            months_used           INTEGER,
            predicted_at          TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customer_loyalty (
            customer_id          TEXT PRIMARY KEY,
            total_loyalty_points INTEGER DEFAULT 0,
            loyalty_tier         TEXT DEFAULT 'Bronze',
            rfm_segment          TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customer_analytics_summary (
            customer_id               TEXT PRIMARY KEY,
            predicted_next_month_spend REAL,
            promotion_sensitivity     TEXT
        )
    """)

    conn.commit()

    yield conn, db_path


@pytest.fixture(scope="function")
def temp_csv_dir():
    """
    Create a temporary directory for CSV files.

    Yields:
        str: Path to temporary directory
    """
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def sample_stores_df():
    """Sample stores DataFrame for testing."""
    return pd.DataFrame({
        'store_id': ['S001', 'S002', 'S003'],
        'store_name': ['Store A', 'Store B', 'Store C'],
        'store_city': ['Mumbai', 'Delhi', 'Chennai'],
        'store_region': ['West', 'North', 'South'],
        'opening_date': ['2020-01-01', '2021-06-15', '2022-03-10']
    })


@pytest.fixture
def sample_products_df():
    """Sample products DataFrame for testing."""
    return pd.DataFrame({
        'product_id': ['P001', 'P002', 'P003'],
        'product_name': ['Product A', 'Product B', 'Product C'],
        'product_category': ['Electronics', 'Grocery', 'Apparel'],
        'unit_price': [1000.0, 50.0, 500.0],
        'current_stock_level': [100, 200, 150],
        'restock_flag': [0, 0, 0]
    })


@pytest.fixture
def sample_customers_df():
    """Sample customer details DataFrame for testing."""
    return pd.DataFrame({
        'customer_id': ['C001', 'C002', 'C003', 'C004'],
        'first_name': ['Alice', 'Bob', 'Charlie', 'Diana'],
        'email': ['alice@test.com', 'bob@test.com', 'charlie@test.com', 'diana@test.com'],
        'loyalty_status': ['Bronze', 'Bronze', 'Bronze', 'Bronze'],
        'total_loyalty_points': [0, 0, 0, 0],
        'last_purchase_date': [None, None, None, None],
        'segment_id': [None, None, None, None],
        'customer_phone': ['9876543210', '9876543211', '9876543212', '9876543213'],
        'customer_since': ['2023-01-01', '2023-02-01', '2023-03-01', '2023-04-01'],
        'promotion_sensitivity': [None, None, None, None]
    })


@pytest.fixture
def sample_transactions_df():
    """Sample transactions DataFrame for testing."""
    return pd.DataFrame({
        'transaction_id': ['T001', 'T002', 'T003', 'T004', 'T005'],
        'customer_id': ['C001', 'C001', 'C002', 'C003', 'C003'],
        'store_id': ['S001', 'S001', 'S002', 'S001', 'S002'],
        'transaction_date': ['2024-01-15', '2024-01-20', '2024-01-18', '2024-01-10', '2024-01-25'],
        'total_amount': [5000.0, 3000.0, 10000.0, 2000.0, 15000.0]
    })


@pytest.fixture
def sample_line_items_df():
    """Sample line items DataFrame for testing."""
    return pd.DataFrame({
        'line_item_id': ['L001', 'L002', 'L003', 'L004', 'L005'],
        'transaction_id': ['T001', 'T001', 'T002', 'T003', 'T004'],
        'product_id': ['P001', 'P002', 'P001', 'P003', 'P002'],
        'promotion_id': ['PR001', None, 'PR002', None, 'PR001'],
        'quantity': [2, 5, 1, 3, 10],
        'line_item_amount': [2000.0, 250.0, 1000.0, 1500.0, 500.0]
    })


@pytest.fixture
def sample_loyalty_rules_df():
    """Sample loyalty rules DataFrame for testing."""
    return pd.DataFrame({
        'rule_id': ['R001'],
        'rule_name': ['Standard'],
        'points_per_unit_spend': [0.01],
        'min_spend_threshold': [1000.0],
        'bonus_points': [50],
        'start_date': ['2024-01-01'],
        'end_date': ['2025-12-31']
    })


@pytest.fixture
def temp_csv_dir():
    """Create a temporary directory for CSV files."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


def insert_sample_data(conn, stores_df, products_df, customers_df,
                       transactions_df, line_items_df, loyalty_rules_df=None):
    """
    Helper function to insert sample data into database.

    Args:
        conn: SQLite database connection
        stores_df: Stores DataFrame
        products_df: Products DataFrame
        customers_df: Customer details DataFrame
        transactions_df: Transactions DataFrame
        line_items_df: Line items DataFrame
        loyalty_rules_df: Optional loyalty rules DataFrame
    """
    stores_df.to_sql('stores', conn, if_exists='append', index=False)
    products_df.to_sql('products', conn, if_exists='append', index=False)
    customers_df.to_sql('customer_details', conn, if_exists='append', index=False)
    transactions_df.to_sql('store_sales_header', conn, if_exists='append', index=False)
    line_items_df.to_sql('store_sales_line_items', conn, if_exists='append', index=False)

    if loyalty_rules_df is not None:
        loyalty_rules_df.to_sql('loyalty_rules', conn, if_exists='append', index=False)

    conn.commit()
