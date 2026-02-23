"""
01_setup_db.py - Database Setup Script for RetailPulse

This script creates all necessary SQLite database tables for the retail analytics system.
It creates 7 core tables and 7 corresponding rejected tables for data quality tracking.

Run this script FIRST before any other scripts in the pipeline.

Usage:
    python src/01_setup_db.py
"""

import sqlite3
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(__file__))

from utils.error_handler import (
    get_logger,
    DatabaseError,
    handle_exceptions,
    validate_directory_exists,
    DatabaseConnection
)

# Initialize logger
logger = get_logger(__name__)

# Database path configuration
DB_PATH = os.path.join(os.path.dirname(__file__), "../db/retail.db")


@handle_exceptions(logger=logger)
def get_connection():
    """
    Establish a connection to the SQLite database.
    Creates the database file if it doesn't exist.

    Returns:
        sqlite3.Connection: Database connection object

    Raises:
        DatabaseError: If connection fails
    """
    try:
        # Ensure the db directory exists
        db_dir = os.path.dirname(DB_PATH)
        validate_directory_exists(db_dir, create=True)
        logger.info(f"Connecting to database: {DB_PATH}")

        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row

        # Test connection
        conn.execute("SELECT 1")
        logger.debug("Database connection successful")
        return conn
    except sqlite3.Error as e:
        logger.error(f"SQLite error while connecting: {e}")
        raise DatabaseError(f"Failed to connect to database: {e}", operation='connect')


@handle_exceptions(logger=logger)
def create_tables(conn):
    """
    Create all core and rejected tables in the database.

    Args:
        conn: SQLite database connection

    Raises:
        DatabaseError: If table creation fails
    """
    if conn is None:
        raise DatabaseError("Database connection is None", operation='create_tables')

    cursor = conn.cursor()
    tables_created = []
    errors = []

    # ============================================
    # CORE TABLES
    # ============================================

    # Table 1: stores
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stores (
            store_id       TEXT PRIMARY KEY,
            store_name     TEXT NOT NULL,
            store_city     TEXT,
            store_region   TEXT,
            opening_date   TEXT
        )
    """)

    # Table 2: products
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

    # Table 3: customer_details (with email and customer_since columns)
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

    # Table 4: store_sales_header
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS store_sales_header (
            transaction_id   TEXT PRIMARY KEY,
            customer_id      TEXT,
            store_id         TEXT,
            transaction_date TEXT,
            total_amount     REAL,
            FOREIGN KEY (customer_id) REFERENCES customer_details(customer_id),
            FOREIGN KEY (store_id)    REFERENCES stores(store_id)
        )
    """)

    # Table 5: store_sales_line_items (using transaction_id as FK column name)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS store_sales_line_items (
            line_item_id      TEXT PRIMARY KEY,
            transaction_id    TEXT,
            product_id        TEXT,
            promotion_id      TEXT,
            quantity          INTEGER,
            line_item_amount  REAL,
            FOREIGN KEY (transaction_id) REFERENCES store_sales_header(transaction_id),
            FOREIGN KEY (product_id)     REFERENCES products(product_id),
            FOREIGN KEY (promotion_id)   REFERENCES promotion_details(promotion_id)
        )
    """)

    # Table 6: promotion_details
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

    # Table 7: loyalty_rules
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

    # ============================================
    # REJECTED TABLES (mirror tables for data quality)
    # ============================================

    # stores_rejected
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stores_rejected (
            store_id       TEXT,
            store_name     TEXT,
            store_city     TEXT,
            store_region   TEXT,
            opening_date   TEXT,
            reject_reason  TEXT
        )
    """)

    # products_rejected
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products_rejected (
            product_id          TEXT,
            product_name        TEXT,
            product_category    TEXT,
            unit_price          REAL,
            current_stock_level INTEGER,
            restock_flag        INTEGER,
            reject_reason       TEXT
        )
    """)

    # customer_details_rejected
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customer_details_rejected (
            customer_id            TEXT,
            first_name             TEXT,
            email                  TEXT,
            loyalty_status         TEXT,
            total_loyalty_points   INTEGER,
            last_purchase_date     TEXT,
            segment_id             TEXT,
            customer_phone         TEXT,
            customer_since         TEXT,
            promotion_sensitivity  TEXT,
            reject_reason          TEXT
        )
    """)

    # store_sales_header_rejected
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS store_sales_header_rejected (
            transaction_id   TEXT,
            customer_id      TEXT,
            store_id         TEXT,
            transaction_date TEXT,
            total_amount     REAL,
            reject_reason    TEXT
        )
    """)

    # store_sales_line_items_rejected
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS store_sales_line_items_rejected (
            line_item_id      TEXT,
            transaction_id    TEXT,
            product_id        TEXT,
            promotion_id      TEXT,
            quantity          INTEGER,
            line_item_amount  REAL,
            reject_reason     TEXT
        )
    """)

    # promotion_details_rejected
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS promotion_details_rejected (
            promotion_id           TEXT,
            promotion_name         TEXT,
            start_date             TEXT,
            end_date               TEXT,
            discount_percentage    REAL,
            applicable_category    TEXT,
            reject_reason          TEXT
        )
    """)

    # loyalty_rules_rejected
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS loyalty_rules_rejected (
            rule_id               TEXT,
            rule_name             TEXT,
            points_per_unit_spend REAL,
            min_spend_threshold   REAL,
            bonus_points          INTEGER,
            start_date            TEXT,
            end_date              TEXT,
            reject_reason         TEXT
        )
    """)

    # ============================================
    # ANALYTICS TABLES (created by later scripts)
    # ============================================

    # RFM Summary table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS rfm_summary (
            customer_id    TEXT PRIMARY KEY,
            recency_days   INTEGER,
            frequency      INTEGER,
            monetary_value REAL,
            calculated_at  TEXT
        )
    """)

    # Customer Predictions table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customer_predictions (
            customer_id           TEXT PRIMARY KEY,
            predicted_next_spend  REAL,
            months_used           INTEGER,
            predicted_at          TEXT
        )
    """)

    conn.commit()
    logger.info("All tables created successfully!")
    print("All tables created successfully!")


def verify_tables(conn):
    """
    Verify that all expected tables were created.

    Args:
        conn: Database connection

    Returns:
        tuple: (success: bool, tables: list)
    """
    expected_tables = [
        'stores', 'products', 'customer_details', 'store_sales_header',
        'store_sales_line_items', 'promotion_details', 'loyalty_rules',
        'stores_rejected', 'products_rejected', 'customer_details_rejected',
        'store_sales_header_rejected', 'store_sales_line_items_rejected',
        'promotion_details_rejected', 'loyalty_rules_rejected',
        'rfm_summary', 'customer_predictions'
    ]

    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [row['name'] for row in cursor.fetchall()]

    missing = set(expected_tables) - set(tables)
    if missing:
        logger.warning(f"Missing tables: {missing}")
        return False, tables

    return True, tables


def main():
    """Main entry point for database setup."""
    logger.info("=" * 60)
    logger.info("RetailPulse Database Setup")
    logger.info("=" * 60)

    conn = None
    exit_code = 0

    try:
        conn = get_connection()
        logger.info(f"Connected to database: {DB_PATH}")
        print(f"Connected to database: {DB_PATH}")

        create_tables(conn)

        # Verify tables were created
        success, tables = verify_tables(conn)

        print(f"\nCreated {len(tables)} tables:")
        for table in tables:
            print(f"  - {table}")

        if success:
            logger.info("Database setup completed successfully")
        else:
            logger.warning("Database setup completed with warnings - some tables may be missing")
            exit_code = 1

    except DatabaseError as e:
        logger.error(f"Database error: {e}")
        print(f"Database error: {e}")
        exit_code = 1
    except sqlite3.Error as e:
        logger.error(f"SQLite error: {e}")
        print(f"SQLite error: {e}")
        exit_code = 1
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        print(f"Unexpected error: {e}")
        exit_code = 1
    finally:
        if conn:
            try:
                conn.close()
                logger.debug("Database connection closed")
                print("\nDatabase connection closed.")
            except Exception as e:
                logger.error(f"Error closing connection: {e}")

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
