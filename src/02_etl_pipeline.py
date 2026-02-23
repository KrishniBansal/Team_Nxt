"""
02_etl_pipeline.py - ETL Pipeline for RetailPulse

This script handles the Extract, Transform, Load (ETL) process for all CSV data files.
It validates data, loads clean records to the database, and tracks rejected records.

Features:
- Null validation for mandatory columns
- Negative value detection
- Special character stripping from numeric fields
- Datatype casting and validation
- Incremental load support (INSERT OR IGNORE)
- Export of clean and rejected records to CSV
- Comprehensive error handling and logging

"""

import sqlite3
import pandas as pd
import os
import sys
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(__file__))

from utils.error_handler import (
    get_logger,
    DatabaseError,
    DataValidationError,
    FileError,
    ETLError,
    handle_exceptions,
    validate_file_exists,
    validate_directory_exists,
)

# Initialize logger
logger = get_logger(__name__)

# Database path configuration
DB_PATH = os.path.join(os.path.dirname(__file__), "../db/retail.db")
DATA_RAW_PATH = os.path.join(os.path.dirname(__file__), "../data/raw")
DATA_CLEANED_PATH = os.path.join(os.path.dirname(__file__), "../data/cleaned")
DATA_REJECTED_PATH = os.path.join(os.path.dirname(__file__), "../data/rejected")

# ============================================
# VALIDATION CONFIGURATION
# ============================================

# Mandatory columns that cannot be null
MANDATORY_COLUMNS = {
    "stores": ["store_id", "store_name"],
    "products": ["product_id", "product_name", "unit_price"],
    "customer_details": ["customer_id", "first_name"],
    "store_sales_header": ["transaction_id", "customer_id", "store_id", "transaction_date", "total_amount"],
    "store_sales_line_items": ["line_item_id", "transaction_id", "product_id", "quantity", "line_item_amount"],
    "promotion_details": ["promotion_id", "promotion_name"],
    "loyalty_rules": ["rule_id", "points_per_unit_spend"],
}

# Numeric columns to check for negative values
NUMERIC_COLUMNS = {
    "products": ["unit_price", "current_stock_level"],
    "store_sales_header": ["total_amount"],
    "store_sales_line_items": ["quantity", "line_item_amount"],
    "loyalty_rules": ["points_per_unit_spend", "min_spend_threshold", "bonus_points"],
}

# Columns that need special character stripping
STRIP_COLUMNS = ["unit_price", "total_amount", "line_item_amount",
                 "points_per_unit_spend", "min_spend_threshold", "discount_percentage"]

# Date columns for type casting
DATE_COLUMNS = ["opening_date", "start_date", "end_date", "transaction_date",
                "last_purchase_date", "customer_since"]

# Real/Float columns for type casting
REAL_COLUMNS = ["unit_price", "total_amount", "line_item_amount",
                "discount_percentage", "points_per_unit_spend", "min_spend_threshold"]

# CSV file mapping to table names
CSV_MAPPING = {
    "stores": "stores.csv",
    "products": "products.csv",
    "customer_details": "customer_details.csv",
    "promotion_details": "promotion_details.csv",
    "loyalty_rules": "loyalty_rules.csv",
    "store_sales_header": "store_sales_header.csv",
    "store_sales_line_items": "store_sales_line_items.csv",
}

# Table column definitions (for ensuring correct column order)
TABLE_COLUMNS = {
    "stores": ["store_id", "store_name", "store_city", "store_region", "opening_date"],
    "products": ["product_id", "product_name", "product_category", "unit_price",
                 "current_stock_level", "restock_flag"],
    "customer_details": ["customer_id", "first_name", "email", "loyalty_status",
                         "total_loyalty_points", "last_purchase_date", "segment_id",
                         "customer_phone", "customer_since", "promotion_sensitivity"],
    "store_sales_header": ["transaction_id", "customer_id", "store_id",
                           "transaction_date", "total_amount"],
    "store_sales_line_items": ["line_item_id", "transaction_id", "product_id",
                               "promotion_id", "quantity", "line_item_amount"],
    "promotion_details": ["promotion_id", "promotion_name", "start_date",
                          "end_date", "discount_percentage", "applicable_category"],
    "loyalty_rules": ["rule_id", "rule_name", "points_per_unit_spend",
                      "min_spend_threshold", "bonus_points", "start_date", "end_date"],
}


def get_connection():
    try:
        if not os.path.exists(DB_PATH):
            raise DatabaseError(
                f"Database file not found. Run 01_setup_db.py first.",
                operation='connect'
            )

        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        logger.debug(f"Connected to database: {DB_PATH}")
        return conn
    except sqlite3.Error as e:
        logger.error(f"Failed to connect to database: {e}")
        raise DatabaseError(f"Database connection failed: {e}", operation='connect')


@handle_exceptions(logger=logger, reraise=False, default_return=None)
def normalize_id_columns(df, table_name):
    # ID columns that need normalization (may come as floats from CSV)
    id_columns = {
        'store_sales_line_items': ['transaction_id', 'promotion_id'],
        'store_sales_header': ['transaction_id'],
    }

    cols_to_normalize = id_columns.get(table_name, [])

    for col in cols_to_normalize:
        if col in df.columns:
            # Convert to string, remove .0 suffix, handle NaN
            df[col] = df[col].apply(lambda x: str(int(float(x))) if pd.notna(x) and str(x).replace('.', '').replace('-', '').isdigit() else (None if pd.isna(x) else str(x)))
            # Replace 'nan' strings with None
            df[col] = df[col].replace(['nan', 'None'], None)

    return df


def strip_special_chars(df, columns):
    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(r'[$₹£€%,\s]', '', regex=True)
            # Replace 'nan' strings with actual NaN
            df[col] = df[col].replace(['nan', 'NaN', 'None', ''], pd.NA)
    return df


def cast_datatypes(df, table_name):
    # Cast REAL columns
    for col in REAL_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Cast DATE columns
    for col in DATE_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            # Convert to string format for SQLite
            df[col] = df[col].dt.strftime('%Y-%m-%d')
            df[col] = df[col].replace('NaT', pd.NA)

    # Cast INTEGER columns
    if 'quantity' in df.columns:
        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
    if 'current_stock_level' in df.columns:
        df['current_stock_level'] = pd.to_numeric(df['current_stock_level'], errors='coerce')
    if 'bonus_points' in df.columns:
        df['bonus_points'] = pd.to_numeric(df['bonus_points'], errors='coerce')
    if 'total_loyalty_points' in df.columns:
        df['total_loyalty_points'] = pd.to_numeric(df['total_loyalty_points'], errors='coerce')
    if 'restock_flag' in df.columns:
        df['restock_flag'] = pd.to_numeric(df['restock_flag'], errors='coerce').fillna(0).astype(int)

    return df


def validate_row(row, table_name, mandatory_cols, numeric_cols):
    # Step 1: Null check for mandatory columns
    for col in mandatory_cols:
        if col in row.index:
            val = row[col]
            if pd.isna(val) or val == '' or str(val).strip() == '':
                return False, f"null {col}"

    # Step 2: Negative value check
    for col in numeric_cols:
        if col in row.index:
            val = row[col]
            if pd.notna(val):
                try:
                    if float(val) < 0:
                        return False, f"negative value in {col}"
                except (ValueError, TypeError):
                    pass

    return True, None


def ingest_csv(table_name, csv_path, conn):
    result = {
        "table": table_name,
        "total": 0,
        "loaded": 0,
        "rejected": 0,
        "errors": []
    }

    # Check if file exists
    if not os.path.exists(csv_path):
        msg = f"File not found: {csv_path}"
        logger.warning(msg)
        print(f"  WARNING: {msg}")
        result["errors"].append(msg)
        return result

    # Read CSV with error handling
    try:
        df = pd.read_csv(csv_path)
        logger.info(f"Read {len(df)} rows from {csv_path}")
    except pd.errors.EmptyDataError:
        msg = f"Empty CSV file: {csv_path}"
        logger.warning(msg)
        result["errors"].append(msg)
        return result
    except pd.errors.ParserError as e:
        msg = f"CSV parsing error in {csv_path}: {e}"
        logger.error(msg)
        result["errors"].append(msg)
        return result
    except Exception as e:
        msg = f"Error reading {csv_path}: {e}"
        logger.error(msg)
        print(f"  ERROR: {msg}")
        result["errors"].append(msg)
        return result

    result["total"] = len(df)

    if len(df) == 0:
        msg = f"Empty file: {csv_path}"
        logger.warning(msg)
        print(f"  WARNING: {msg}")
        return result

    # Get validation config for this table
    mandatory_cols = MANDATORY_COLUMNS.get(table_name, [])
    numeric_cols = NUMERIC_COLUMNS.get(table_name, [])

    # Normalize ID columns (convert float IDs like '1.0' to '1')
    try:
        df = normalize_id_columns(df, table_name)
    except Exception as e:
        logger.warning(f"Error normalizing ID columns for {table_name}: {e}")

    # Step 3: Strip special characters
    try:
        df = strip_special_chars(df, STRIP_COLUMNS)
    except Exception as e:
        logger.warning(f"Error stripping special chars for {table_name}: {e}")

    # Step 4: Cast datatypes
    try:
        df = cast_datatypes(df, table_name)
    except Exception as e:
        logger.warning(f"Error casting datatypes for {table_name}: {e}")

    # Prepare lists for clean and rejected records
    clean_records = []
    rejected_records = []

    # Validate each row
    for idx, row in df.iterrows():
        try:
            is_valid, reject_reason = validate_row(row, table_name, mandatory_cols, numeric_cols)

            # Additional datatype validation for mandatory columns
            if is_valid:
                for col in mandatory_cols:
                    if col in row.index:
                        val = row[col]
                        if pd.isna(val):
                            is_valid = False
                            reject_reason = f"invalid datatype in {col}"
                            break

            if is_valid:
                clean_records.append(row)
            else:
                row_with_reason = row.copy()
                row_with_reason['reject_reason'] = reject_reason
                rejected_records.append(row_with_reason)
        except Exception as e:
            logger.warning(f"Error validating row {idx} in {table_name}: {e}")
            row_with_reason = row.copy()
            row_with_reason['reject_reason'] = f"validation error: {str(e)[:50]}"
            rejected_records.append(row_with_reason)

    # Create DataFrames
    expected_cols = TABLE_COLUMNS.get(table_name, df.columns.tolist())

    if clean_records:
        df_clean = pd.DataFrame(clean_records)
        # Ensure only expected columns are included
        available_cols = [col for col in expected_cols if col in df_clean.columns]
        df_clean = df_clean[available_cols]
    else:
        df_clean = pd.DataFrame(columns=expected_cols)

    if rejected_records:
        df_rejected = pd.DataFrame(rejected_records)
    else:
        df_rejected = pd.DataFrame(columns=expected_cols + ['reject_reason'])

    # Load clean records to database using INSERT OR IGNORE for incremental support
    if len(df_clean) > 0:
        try:
            # Use INSERT OR IGNORE to handle duplicates
            for _, row in df_clean.iterrows():
                placeholders = ', '.join(['?' for _ in available_cols])
                cols_str = ', '.join(available_cols)
                sql = f"INSERT OR IGNORE INTO {table_name} ({cols_str}) VALUES ({placeholders})"
                conn.execute(sql, tuple(row[col] for col in available_cols))
            conn.commit()
            result["loaded"] = len(df_clean)
        except Exception as e:
            print(f"  ERROR loading to {table_name}: {e}")
            conn.rollback()

    # Load rejected records to rejected table
    if len(df_rejected) > 0:
        try:
            df_rejected.to_sql(f"{table_name}_rejected", conn, if_exists='append', index=False)
            conn.commit()
            result["rejected"] = len(df_rejected)
        except Exception as e:
            print(f"  ERROR loading to {table_name}_rejected: {e}")

    # Export to CSV files
    os.makedirs(DATA_CLEANED_PATH, exist_ok=True)
    os.makedirs(DATA_REJECTED_PATH, exist_ok=True)

    df_clean.to_csv(os.path.join(DATA_CLEANED_PATH, f"{table_name}_clean.csv"), index=False)
    df_rejected.to_csv(os.path.join(DATA_REJECTED_PATH, f"{table_name}_rejected.csv"), index=False)

    return result


def print_summary(results):
    print("\n" + "=" * 70)
    print("ETL PIPELINE SUMMARY")
    print("=" * 70)
    print(f"{'Table':<30} {'Total':>10} {'Loaded':>10} {'Rejected':>10}")
    print("-" * 70)

    total_all = 0
    loaded_all = 0
    rejected_all = 0

    for r in results:
        print(f"{r['table']:<30} {r['total']:>10} {r['loaded']:>10} {r['rejected']:>10}")
        total_all += r['total']
        loaded_all += r['loaded']
        rejected_all += r['rejected']

    print("-" * 70)
    print(f"{'TOTAL':<30} {total_all:>10} {loaded_all:>10} {rejected_all:>10}")
    print("=" * 70)


def main():
    """Main entry point for ETL pipeline."""
    logger.info("=" * 70)
    logger.info("RetailPulse ETL Pipeline Started")
    print("=" * 70)
    print("RetailPulse ETL Pipeline")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    conn = None
    results = []
    exit_code = 0
    total_errors = []

    try:
        # Validate directories exist
        validate_directory_exists(DATA_CLEANED_PATH, create=True)
        validate_directory_exists(DATA_REJECTED_PATH, create=True)

        conn = get_connection()
        logger.info(f"Connected to database: {DB_PATH}")
        print(f"Connected to database: {DB_PATH}\n")

        # Process tables in dependency order
        table_order = [
            "stores",
            "products",
            "promotion_details",
            "loyalty_rules",
            "customer_details",
            "store_sales_header",
            "store_sales_line_items"
        ]

        for table_name in table_order:
            csv_file = CSV_MAPPING.get(table_name)
            if csv_file:
                csv_path = os.path.join(DATA_RAW_PATH, csv_file)
                logger.info(f"Processing {csv_file}...")
                print(f"Processing {csv_file}...")

                try:
                    result = ingest_csv(table_name, csv_path, conn)
                    results.append(result)

                    if result.get('errors'):
                        total_errors.extend(result['errors'])

                    logger.info(f"  {table_name}: Loaded {result['loaded']}, Rejected {result['rejected']}")
                    print(f"  Loaded: {result['loaded']}, Rejected: {result['rejected']}")
                except ETLError as e:
                    logger.error(f"ETL error processing {table_name}: {e}")
                    total_errors.append(str(e))
                    results.append({
                        "table": table_name,
                        "total": 0,
                        "loaded": 0,
                        "rejected": 0,
                        "errors": [str(e)]
                    })

        print_summary(results)

        if total_errors:
            logger.warning(f"ETL completed with {len(total_errors)} warning(s)")
            exit_code = 1

    except DatabaseError as e:
        logger.error(f"Database error: {e}")
        print(f"Database error: {e}")
        exit_code = 1
    except FileError as e:
        logger.error(f"File error: {e}")
        print(f"File error: {e}")
        exit_code = 1
    except sqlite3.Error as e:
        logger.error(f"SQLite error: {e}")
        print(f"SQLite error: {e}")
        exit_code = 1
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        print(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        exit_code = 1
    finally:
        if conn:
            try:
                conn.close()
                logger.debug("Database connection closed")
                print("\nDatabase connection closed.")
            except Exception as e:
                logger.error(f"Error closing connection: {e}")

    print(f"\nClean data exported to: {DATA_CLEANED_PATH}")
    print(f"Rejected data exported to: {DATA_REJECTED_PATH}")

    logger.info(f"ETL Pipeline completed with exit code: {exit_code}")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
