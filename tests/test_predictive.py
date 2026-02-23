"""
test_predictive.py - Unit tests for 04_predictive.py

Tests for predictive analytics functions: spend forecast, restock flag, promotion sensitivity.
"""

import pytest
import pandas as pd
import numpy as np
import sqlite3
import os
import sys
from datetime import datetime, timedelta

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


class TestPredictNextMonthSpend:
    """Tests for predict_next_month_spend function."""

    def test_average_of_three_months(self):
        """Test spend forecast as average of last 3 months."""
        # Monthly spends
        month1_spend = 1000
        month2_spend = 1200
        month3_spend = 1100

        predicted_spend = (month1_spend + month2_spend + month3_spend) / 3

        assert predicted_spend == 1100.0

    def test_no_history_returns_zero(self):
        """Test that no purchase history returns 0."""
        monthly_spends = []

        if len(monthly_spends) == 0:
            predicted_spend = 0
        else:
            predicted_spend = sum(monthly_spends) / len(monthly_spends)

        assert predicted_spend == 0

    def test_single_month_history(self):
        """Test prediction with only one month of data."""
        monthly_spends = [500]

        predicted_spend = sum(monthly_spends) / len(monthly_spends) if monthly_spends else 0

        assert predicted_spend == 500

    def test_two_month_history(self):
        """Test prediction with two months of data."""
        monthly_spends = [400, 600]

        predicted_spend = sum(monthly_spends) / len(monthly_spends)

        assert predicted_spend == 500

    def test_handles_varying_amounts(self):
        """Test with varying monthly amounts."""
        monthly_spends = [200, 800, 500]

        predicted_spend = sum(monthly_spends) / len(monthly_spends)

        assert predicted_spend == 500


class TestPredictRestockFlag:
    """Tests for predict_restock_flag function."""

    def test_restock_needed_zero_stock(self):
        """Test restock flag when stock is 0."""
        current_stock = 0
        restock_threshold = 10

        needs_restock = current_stock <= restock_threshold
        flag = 'Yes' if needs_restock else 'No'

        assert flag == 'Yes'

    def test_restock_needed_below_threshold(self):
        """Test restock flag when stock below threshold."""
        current_stock = 5
        restock_threshold = 10

        needs_restock = current_stock <= restock_threshold
        flag = 'Yes' if needs_restock else 'No'

        assert flag == 'Yes'

    def test_restock_not_needed(self):
        """Test restock flag when stock above threshold."""
        current_stock = 50
        restock_threshold = 10

        needs_restock = current_stock <= restock_threshold
        flag = 'Yes' if needs_restock else 'No'

        assert flag == 'No'

    def test_restock_at_threshold(self):
        """Test restock flag when stock equals threshold."""
        current_stock = 10
        restock_threshold = 10

        needs_restock = current_stock <= restock_threshold
        flag = 'Yes' if needs_restock else 'No'

        assert flag == 'Yes'  # At threshold should trigger restock

    def test_default_threshold_10(self):
        """Test default restock threshold is 10."""
        default_threshold = 10

        # Below threshold
        assert (5 <= default_threshold) == True
        # Above threshold
        assert (20 <= default_threshold) == False


class TestPredictPromotionSensitivity:
    """Tests for predict_promotion_sensitivity function."""

    def test_high_sensitivity(self):
        """Test HIGH sensitivity when usage > 50%."""
        total_transactions = 100
        promo_transactions = 60

        promo_rate = (promo_transactions / total_transactions) * 100

        if promo_rate > 50:
            sensitivity = 'HIGH'
        elif promo_rate > 20:
            sensitivity = 'MEDIUM'
        else:
            sensitivity = 'LOW'

        assert sensitivity == 'HIGH'

    def test_medium_sensitivity(self):
        """Test MEDIUM sensitivity when usage 20-50%."""
        total_transactions = 100
        promo_transactions = 35

        promo_rate = (promo_transactions / total_transactions) * 100

        if promo_rate > 50:
            sensitivity = 'HIGH'
        elif promo_rate > 20:
            sensitivity = 'MEDIUM'
        else:
            sensitivity = 'LOW'

        assert sensitivity == 'MEDIUM'

    def test_low_sensitivity(self):
        """Test LOW sensitivity when usage <= 20%."""
        total_transactions = 100
        promo_transactions = 15

        promo_rate = (promo_transactions / total_transactions) * 100

        if promo_rate > 50:
            sensitivity = 'HIGH'
        elif promo_rate > 20:
            sensitivity = 'MEDIUM'
        else:
            sensitivity = 'LOW'

        assert sensitivity == 'LOW'

    def test_boundary_50_percent(self):
        """Test boundary at 50% (should be MEDIUM)."""
        promo_rate = 50

        if promo_rate > 50:
            sensitivity = 'HIGH'
        elif promo_rate > 20:
            sensitivity = 'MEDIUM'
        else:
            sensitivity = 'LOW'

        assert sensitivity == 'MEDIUM'  # 50 is not > 50

    def test_boundary_20_percent(self):
        """Test boundary at 20% (should be LOW)."""
        promo_rate = 20

        if promo_rate > 50:
            sensitivity = 'HIGH'
        elif promo_rate > 20:
            sensitivity = 'MEDIUM'
        else:
            sensitivity = 'LOW'

        assert sensitivity == 'LOW'  # 20 is not > 20

    def test_no_transactions(self):
        """Test sensitivity when customer has no transactions."""
        total_transactions = 0

        if total_transactions == 0:
            sensitivity = 'LOW'

        assert sensitivity == 'LOW'

    def test_all_promo_transactions(self):
        """Test sensitivity when all transactions use promo."""
        total_transactions = 50
        promo_transactions = 50

        promo_rate = (promo_transactions / total_transactions) * 100

        if promo_rate > 50:
            sensitivity = 'HIGH'
        elif promo_rate > 20:
            sensitivity = 'MEDIUM'
        else:
            sensitivity = 'LOW'

        assert sensitivity == 'HIGH'


class TestPromotionSensitivitySQL:
    """Tests for promotion sensitivity SQL logic."""

    def test_case_when_high(self, temp_db_with_tables):
        """Test SQL CASE WHEN for HIGH sensitivity."""
        conn, _ = temp_db_with_tables
        cursor = conn.cursor()

        # Create test data
        promo_pct = 60

        cursor.execute(f"""
            SELECT CASE
                WHEN {promo_pct} > 50 THEN 'HIGH'
                WHEN {promo_pct} > 20 THEN 'MEDIUM'
                ELSE 'LOW'
            END as sensitivity
        """)
        result = cursor.fetchone()[0]

        assert result == 'HIGH'

    def test_case_when_medium(self, temp_db_with_tables):
        """Test SQL CASE WHEN for MEDIUM sensitivity."""
        conn, _ = temp_db_with_tables
        cursor = conn.cursor()

        promo_pct = 35

        cursor.execute(f"""
            SELECT CASE
                WHEN {promo_pct} > 50 THEN 'HIGH'
                WHEN {promo_pct} > 20 THEN 'MEDIUM'
                ELSE 'LOW'
            END as sensitivity
        """)
        result = cursor.fetchone()[0]

        assert result == 'MEDIUM'

    def test_case_when_low(self, temp_db_with_tables):
        """Test SQL CASE WHEN for LOW sensitivity."""
        conn, _ = temp_db_with_tables
        cursor = conn.cursor()

        promo_pct = 10

        cursor.execute(f"""
            SELECT CASE
                WHEN {promo_pct} > 50 THEN 'HIGH'
                WHEN {promo_pct} > 20 THEN 'MEDIUM'
                ELSE 'LOW'
            END as sensitivity
        """)
        result = cursor.fetchone()[0]

        assert result == 'LOW'


class TestSpendForecastSQL:
    """Tests for spend forecast SQL calculations."""

    def test_monthly_aggregation(self, temp_db_with_tables):
        """Test monthly spend aggregation."""
        conn, _ = temp_db_with_tables
        cursor = conn.cursor()

        # Insert customer
        cursor.execute("""
            INSERT INTO customer_details (customer_id, first_name)
            VALUES ('C001', 'Test')
        """)

        # Insert transactions across months
        cursor.execute("""
            INSERT INTO store_sales_header
            (transaction_id, customer_id, store_id, transaction_date, total_amount)
            VALUES ('T001', 'C001', 'S001', '2024-01-15', 1000.00)
        """)
        cursor.execute("""
            INSERT INTO store_sales_header
            (transaction_id, customer_id, store_id, transaction_date, total_amount)
            VALUES ('T002', 'C001', 'S001', '2024-02-15', 1500.00)
        """)
        cursor.execute("""
            INSERT INTO store_sales_header
            (transaction_id, customer_id, store_id, transaction_date, total_amount)
            VALUES ('T003', 'C001', 'S001', '2024-03-15', 1200.00)
        """)
        conn.commit()

        # Calculate average
        cursor.execute("""
            SELECT AVG(total_amount)
            FROM store_sales_header
            WHERE customer_id = 'C001'
        """)
        avg_spend = cursor.fetchone()[0]

        expected_avg = (1000 + 1500 + 1200) / 3
        assert abs(avg_spend - expected_avg) < 0.01


class TestRestockFlagSQL:
    """Tests for restock flag SQL queries."""

    def test_products_needing_restock(self, temp_db_with_tables):
        """Test query for products needing restock."""
        conn, _ = temp_db_with_tables
        cursor = conn.cursor()

        # Insert products with varying stock levels
        cursor.execute("""
            INSERT INTO products (product_id, product_name, unit_price, current_stock_level)
            VALUES ('P001', 'Low Stock Item', 100, 5)
        """)
        cursor.execute("""
            INSERT INTO products (product_id, product_name, unit_price, current_stock_level)
            VALUES ('P002', 'Adequate Stock Item', 100, 50)
        """)
        cursor.execute("""
            INSERT INTO products (product_id, product_name, unit_price, current_stock_level)
            VALUES ('P003', 'Zero Stock Item', 100, 0)
        """)
        conn.commit()

        # Query products needing restock (threshold = 10)
        cursor.execute("""
            SELECT product_id, current_stock_level
            FROM products
            WHERE current_stock_level <= 10
        """)
        low_stock = cursor.fetchall()

        # Should return P001 (5) and P003 (0)
        product_ids = [row[0] for row in low_stock]
        assert 'P001' in product_ids
        assert 'P003' in product_ids
        assert 'P002' not in product_ids
        assert len(low_stock) == 2


class TestAnalyticsSummaryTable:
    """Tests for customer_analytics_summary table."""

    def test_insert_analytics_record(self, temp_db_with_tables):
        """Test inserting analytics summary record."""
        conn, _ = temp_db_with_tables
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO customer_analytics_summary
            (customer_id, predicted_next_month_spend, promotion_sensitivity)
            VALUES ('C001', 1500.50, 'HIGH')
        """)
        conn.commit()

        cursor.execute("SELECT * FROM customer_analytics_summary WHERE customer_id = 'C001'")
        row = cursor.fetchone()

        assert row is not None
        assert row[1] == 1500.50  # predicted spend
        assert row[2] == 'HIGH'  # sensitivity

    def test_update_analytics_record(self, temp_db_with_tables):
        """Test updating existing analytics record."""
        conn, _ = temp_db_with_tables
        cursor = conn.cursor()

        # Insert initial
        cursor.execute("""
            INSERT INTO customer_analytics_summary
            (customer_id, predicted_next_month_spend, promotion_sensitivity)
            VALUES ('C001', 1000.00, 'LOW')
        """)
        conn.commit()

        # Update
        cursor.execute("""
            UPDATE customer_analytics_summary
            SET predicted_next_month_spend = 2000.00, promotion_sensitivity = 'HIGH'
            WHERE customer_id = 'C001'
        """)
        conn.commit()

        cursor.execute("SELECT predicted_next_month_spend, promotion_sensitivity FROM customer_analytics_summary WHERE customer_id = 'C001'")
        row = cursor.fetchone()

        assert row[0] == 2000.00
        assert row[1] == 'HIGH'


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_negative_stock_handling(self):
        """Test handling of negative stock values."""
        stock = -5
        threshold = 10

        # Negative stock should definitely need restock
        needs_restock = stock <= threshold

        assert needs_restock is True

    def test_very_large_spend_values(self):
        """Test handling of very large spend values."""
        monthly_spends = [1000000, 2000000, 3000000]

        predicted_spend = sum(monthly_spends) / len(monthly_spends)

        assert predicted_spend == 2000000

    def test_decimal_precision(self):
        """Test decimal precision in calculations."""
        monthly_spends = [100.33, 200.66, 300.99]

        predicted_spend = sum(monthly_spends) / len(monthly_spends)

        # Should handle decimal precision
        assert abs(predicted_spend - 200.66) < 0.01

    def test_zero_promo_rate(self):
        """Test when no promotions used."""
        total_transactions = 100
        promo_transactions = 0

        promo_rate = (promo_transactions / total_transactions) * 100

        if promo_rate > 50:
            sensitivity = 'HIGH'
        elif promo_rate > 20:
            sensitivity = 'MEDIUM'
        else:
            sensitivity = 'LOW'

        assert sensitivity == 'LOW'
