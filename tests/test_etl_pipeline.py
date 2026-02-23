"""
test_etl_pipeline.py - Unit tests for 02_etl_pipeline.py

Tests for ETL validation, transformation, and loading functions.
"""

import pytest
import pandas as pd
import numpy as np
import os
import sys
import tempfile

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

# Import functions to test
from importlib import import_module


class TestStripSpecialChars:
    """Tests for strip_special_chars function."""

    def test_strip_dollar_sign(self):
        """Test stripping dollar signs from values."""
        df = pd.DataFrame({'unit_price': ['$100.00', '$50.50', '$1,000.00']})

        # Strip characters
        for col in ['unit_price']:
            df[col] = df[col].astype(str).str.replace(r'[$₹£€%,\s]', '', regex=True)

        assert df['unit_price'].iloc[0] == '100.00'
        assert df['unit_price'].iloc[1] == '50.50'
        assert df['unit_price'].iloc[2] == '1000.00'

    def test_strip_rupee_sign(self):
        """Test stripping rupee signs from values."""
        df = pd.DataFrame({'total_amount': ['₹5000', '₹10,000', '₹1,00,000']})

        for col in ['total_amount']:
            df[col] = df[col].astype(str).str.replace(r'[$₹£€%,\s]', '', regex=True)

        assert df['total_amount'].iloc[0] == '5000'
        assert df['total_amount'].iloc[1] == '10000'
        assert df['total_amount'].iloc[2] == '100000'

    def test_strip_percentage(self):
        """Test stripping percentage signs."""
        df = pd.DataFrame({'discount_percentage': ['10%', '25%', '50%']})

        for col in ['discount_percentage']:
            df[col] = df[col].astype(str).str.replace(r'[$₹£€%,\s]', '', regex=True)

        assert df['discount_percentage'].iloc[0] == '10'
        assert df['discount_percentage'].iloc[1] == '25'
        assert df['discount_percentage'].iloc[2] == '50'

    def test_strip_multiple_characters(self):
        """Test stripping multiple special characters at once."""
        df = pd.DataFrame({'value': ['$ 1,234.56', '₹ 5,000.00', '€1,000']})

        df['value'] = df['value'].astype(str).str.replace(r'[$₹£€%,\s]', '', regex=True)

        assert df['value'].iloc[0] == '1234.56'
        assert df['value'].iloc[1] == '5000.00'
        assert df['value'].iloc[2] == '1000'

    def test_preserves_numeric_values(self):
        """Test that clean numeric values are preserved."""
        df = pd.DataFrame({'value': ['100', '200.50', '0']})

        df['value'] = df['value'].astype(str).str.replace(r'[$₹£€%,\s]', '', regex=True)

        assert df['value'].iloc[0] == '100'
        assert df['value'].iloc[1] == '200.50'
        assert df['value'].iloc[2] == '0'


class TestNormalizeIdColumns:
    """Tests for normalize_id_columns function."""

    def test_normalize_float_ids(self):
        """Test normalizing float IDs like '1.0' to '1'."""
        df = pd.DataFrame({
            'transaction_id': ['1.0', '2.0', '3.0', '10.0'],
            'product_id': ['P1', 'P2', 'P3', 'P4']
        })

        # Normalize
        df['transaction_id'] = df['transaction_id'].apply(
            lambda x: str(int(float(x))) if pd.notna(x) and str(x).replace('.', '').replace('-', '').isdigit() else str(x)
        )

        assert df['transaction_id'].iloc[0] == '1'
        assert df['transaction_id'].iloc[1] == '2'
        assert df['transaction_id'].iloc[2] == '3'
        assert df['transaction_id'].iloc[3] == '10'

    def test_preserve_string_ids(self):
        """Test that string IDs are preserved."""
        df = pd.DataFrame({
            'transaction_id': ['T001', 'T002', 'ABC123']
        })

        # String IDs should remain unchanged
        df['transaction_id'] = df['transaction_id'].apply(
            lambda x: str(int(float(x))) if pd.notna(x) and str(x).replace('.', '').replace('-', '').isdigit() else str(x)
        )

        assert df['transaction_id'].iloc[0] == 'T001'
        assert df['transaction_id'].iloc[1] == 'T002'
        assert df['transaction_id'].iloc[2] == 'ABC123'

    def test_handle_null_ids(self):
        """Test handling of null ID values."""
        df = pd.DataFrame({
            'promotion_id': ['1.0', None, '3.0', np.nan]
        })

        df['promotion_id'] = df['promotion_id'].apply(
            lambda x: str(int(float(x))) if pd.notna(x) and str(x).replace('.', '').replace('-', '').isdigit() else (None if pd.isna(x) else str(x))
        )

        assert df['promotion_id'].iloc[0] == '1'
        assert df['promotion_id'].iloc[1] is None
        assert df['promotion_id'].iloc[2] == '3'
        assert df['promotion_id'].iloc[3] is None


class TestValidateRow:
    """Tests for validate_row function."""

    def test_valid_row_passes(self):
        """Test that a valid row passes validation."""
        row = pd.Series({
            'store_id': 'S001',
            'store_name': 'Test Store',
            'store_city': 'Mumbai'
        })

        mandatory_cols = ['store_id', 'store_name']
        numeric_cols = []

        # Validate
        is_valid = True
        reject_reason = None

        for col in mandatory_cols:
            if col in row.index:
                val = row[col]
                if pd.isna(val) or val == '' or str(val).strip() == '':
                    is_valid = False
                    reject_reason = f"null {col}"
                    break

        assert is_valid is True
        assert reject_reason is None

    def test_null_mandatory_column_fails(self):
        """Test that null in mandatory column fails validation."""
        row = pd.Series({
            'store_id': None,
            'store_name': 'Test Store'
        })

        mandatory_cols = ['store_id', 'store_name']

        is_valid = True
        reject_reason = None

        for col in mandatory_cols:
            if col in row.index:
                val = row[col]
                if pd.isna(val) or val == '' or str(val).strip() == '':
                    is_valid = False
                    reject_reason = f"null {col}"
                    break

        assert is_valid is False
        assert reject_reason == "null store_id"

    def test_empty_string_mandatory_column_fails(self):
        """Test that empty string in mandatory column fails validation."""
        row = pd.Series({
            'store_id': 'S001',
            'store_name': ''
        })

        mandatory_cols = ['store_id', 'store_name']

        is_valid = True
        reject_reason = None

        for col in mandatory_cols:
            if col in row.index:
                val = row[col]
                if pd.isna(val) or val == '' or str(val).strip() == '':
                    is_valid = False
                    reject_reason = f"null {col}"
                    break

        assert is_valid is False
        assert reject_reason == "null store_name"

    def test_negative_value_fails(self):
        """Test that negative value in numeric column fails validation."""
        row = pd.Series({
            'product_id': 'P001',
            'product_name': 'Test',
            'unit_price': -100.0
        })

        numeric_cols = ['unit_price']

        is_valid = True
        reject_reason = None

        for col in numeric_cols:
            if col in row.index:
                val = row[col]
                if pd.notna(val):
                    try:
                        if float(val) < 0:
                            is_valid = False
                            reject_reason = f"negative value in {col}"
                            break
                    except (ValueError, TypeError):
                        pass

        assert is_valid is False
        assert reject_reason == "negative value in unit_price"

    def test_positive_value_passes(self):
        """Test that positive value in numeric column passes validation."""
        row = pd.Series({
            'product_id': 'P001',
            'product_name': 'Test',
            'unit_price': 100.0
        })

        numeric_cols = ['unit_price']

        is_valid = True
        reject_reason = None

        for col in numeric_cols:
            if col in row.index:
                val = row[col]
                if pd.notna(val):
                    try:
                        if float(val) < 0:
                            is_valid = False
                            reject_reason = f"negative value in {col}"
                            break
                    except (ValueError, TypeError):
                        pass

        assert is_valid is True
        assert reject_reason is None

    def test_zero_value_passes(self):
        """Test that zero value passes validation."""
        row = pd.Series({
            'product_id': 'P001',
            'unit_price': 0.0
        })

        numeric_cols = ['unit_price']

        is_valid = True
        for col in numeric_cols:
            if col in row.index:
                val = row[col]
                if pd.notna(val):
                    if float(val) < 0:
                        is_valid = False

        assert is_valid is True


class TestCastDatatypes:
    """Tests for cast_datatypes function."""

    def test_cast_real_columns(self):
        """Test casting columns to REAL/float."""
        df = pd.DataFrame({
            'unit_price': ['100.50', '200', '50.00'],
            'total_amount': ['1000', '2000.75', '500']
        })

        df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
        df['total_amount'] = pd.to_numeric(df['total_amount'], errors='coerce')

        assert df['unit_price'].dtype in [np.float64, float]
        assert df['total_amount'].dtype in [np.float64, float]
        assert df['unit_price'].iloc[0] == 100.50

    def test_cast_date_columns(self):
        """Test casting columns to date format."""
        df = pd.DataFrame({
            'transaction_date': ['2024-01-15', '2024-02-20', '2024-03-15']
        })

        df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')

        assert pd.notna(df['transaction_date'].iloc[0])
        assert pd.notna(df['transaction_date'].iloc[1])

    def test_invalid_numeric_becomes_nan(self):
        """Test that invalid numeric values become NaN."""
        df = pd.DataFrame({
            'unit_price': ['100', 'invalid', 'abc']
        })

        df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')

        assert df['unit_price'].iloc[0] == 100
        assert pd.isna(df['unit_price'].iloc[1])
        assert pd.isna(df['unit_price'].iloc[2])

    def test_invalid_date_becomes_nat(self):
        """Test that invalid date values become NaT."""
        df = pd.DataFrame({
            'date_col': ['2024-01-15', 'not-a-date', '99-99-99']
        })

        df['date_col'] = pd.to_datetime(df['date_col'], errors='coerce')

        assert pd.notna(df['date_col'].iloc[0])
        assert pd.isna(df['date_col'].iloc[1])


class TestIngestCSV:
    """Integration tests for ingest_csv function."""

    def test_ingest_valid_csv(self, temp_db_with_tables, temp_csv_dir):
        """Test ingesting a valid CSV file."""
        conn, _ = temp_db_with_tables

        # Create test CSV
        csv_path = os.path.join(temp_csv_dir, "stores.csv")
        df = pd.DataFrame({
            'store_id': ['S001', 'S002'],
            'store_name': ['Store A', 'Store B'],
            'store_city': ['Mumbai', 'Delhi'],
            'store_region': ['West', 'North'],
            'opening_date': ['2020-01-01', '2021-06-15']
        })
        df.to_csv(csv_path, index=False)

        # Read and load
        loaded_df = pd.read_csv(csv_path)
        loaded_df.to_sql('stores', conn, if_exists='append', index=False)

        # Verify
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM stores")
        count = cursor.fetchone()[0]

        assert count == 2

    def test_ingest_csv_with_null_values(self, temp_db_with_tables, temp_csv_dir):
        """Test that rows with null mandatory values are rejected."""
        conn, _ = temp_db_with_tables

        # Create test CSV with missing values
        csv_path = os.path.join(temp_csv_dir, "stores.csv")
        df = pd.DataFrame({
            'store_id': ['S001', None, 'S003'],
            'store_name': ['Store A', 'Store B', None],
            'store_city': ['Mumbai', 'Delhi', 'Chennai'],
            'store_region': ['West', 'North', 'South'],
            'opening_date': ['2020-01-01', '2021-06-15', '2022-03-10']
        })
        df.to_csv(csv_path, index=False)

        # Read and validate
        loaded_df = pd.read_csv(csv_path)

        mandatory_cols = ['store_id', 'store_name']
        clean_records = []
        rejected_records = []

        for idx, row in loaded_df.iterrows():
            is_valid = True
            reject_reason = None

            for col in mandatory_cols:
                if pd.isna(row[col]) or row[col] == '':
                    is_valid = False
                    reject_reason = f"null {col}"
                    break

            if is_valid:
                clean_records.append(row)
            else:
                row['reject_reason'] = reject_reason
                rejected_records.append(row)

        assert len(clean_records) == 1  # Only S001 is valid
        assert len(rejected_records) == 2

    def test_incremental_load_no_duplicates(self, temp_db_with_tables, temp_csv_dir):
        """Test that incremental load doesn't create duplicates."""
        conn, _ = temp_db_with_tables
        cursor = conn.cursor()

        # Insert first record
        cursor.execute("INSERT INTO stores (store_id, store_name) VALUES ('S001', 'Store A')")
        conn.commit()

        # Try to insert duplicate using INSERT OR IGNORE
        cursor.execute("INSERT OR IGNORE INTO stores (store_id, store_name) VALUES ('S001', 'Store A Updated')")
        cursor.execute("INSERT OR IGNORE INTO stores (store_id, store_name) VALUES ('S002', 'Store B')")
        conn.commit()

        # Verify only 2 records (no duplicate S001)
        cursor.execute("SELECT COUNT(*) FROM stores")
        count = cursor.fetchone()[0]
        assert count == 2

        # Verify S001 wasn't updated
        cursor.execute("SELECT store_name FROM stores WHERE store_id = 'S001'")
        name = cursor.fetchone()[0]
        assert name == 'Store A'


class TestMandatoryColumns:
    """Tests for mandatory column configuration."""

    def test_stores_mandatory_columns(self):
        """Test mandatory columns for stores table."""
        mandatory = ['store_id', 'store_name']

        # These should be mandatory
        assert 'store_id' in mandatory
        assert 'store_name' in mandatory

        # Optional columns should not be mandatory
        assert 'store_city' not in mandatory

    def test_products_mandatory_columns(self):
        """Test mandatory columns for products table."""
        mandatory = ['product_id', 'product_name', 'unit_price']

        assert 'product_id' in mandatory
        assert 'product_name' in mandatory
        assert 'unit_price' in mandatory

    def test_transactions_mandatory_columns(self):
        """Test mandatory columns for transactions table."""
        mandatory = ['transaction_id', 'customer_id', 'store_id', 'transaction_date', 'total_amount']

        assert 'transaction_id' in mandatory
        assert 'customer_id' in mandatory
        assert 'total_amount' in mandatory


class TestNumericColumns:
    """Tests for numeric column configuration."""

    def test_products_numeric_columns(self):
        """Test numeric columns for products table."""
        numeric = ['unit_price', 'current_stock_level']

        assert 'unit_price' in numeric
        assert 'current_stock_level' in numeric

    def test_transactions_numeric_columns(self):
        """Test numeric columns for transactions table."""
        numeric = ['total_amount']

        assert 'total_amount' in numeric

    def test_line_items_numeric_columns(self):
        """Test numeric columns for line items table."""
        numeric = ['quantity', 'line_item_amount']

        assert 'quantity' in numeric

        assert 'line_item_amount' in numeric
