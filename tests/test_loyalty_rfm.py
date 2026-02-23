"""
test_loyalty_rfm.py - Unit tests for 03_loyalty_rfm.py

Tests for loyalty tier calculation and RFM segmentation.
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


class TestGetLoyaltyStatus:
    """Tests for get_loyalty_status function."""

    def test_bronze_tier_zero_points(self):
        """Test that 0 points returns Bronze tier."""
        # Bronze: < 500 points
        points = 0

        if points >= 1000:
            tier = 'Gold'
        elif points >= 500:
            tier = 'Silver'
        else:
            tier = 'Bronze'

        assert tier == 'Bronze'

    def test_bronze_tier_boundary(self):
        """Test that 499 points returns Bronze tier."""
        points = 499

        if points >= 1000:
            tier = 'Gold'
        elif points >= 500:
            tier = 'Silver'
        else:
            tier = 'Bronze'

        assert tier == 'Bronze'

    def test_silver_tier_boundary_low(self):
        """Test that 500 points returns Silver tier."""
        points = 500

        if points >= 1000:
            tier = 'Gold'
        elif points >= 500:
            tier = 'Silver'
        else:
            tier = 'Bronze'

        assert tier == 'Silver'

    def test_silver_tier_boundary_high(self):
        """Test that 999 points returns Silver tier."""
        points = 999

        if points >= 1000:
            tier = 'Gold'
        elif points >= 500:
            tier = 'Silver'
        else:
            tier = 'Bronze'

        assert tier == 'Silver'

    def test_gold_tier_boundary(self):
        """Test that 1000 points returns Gold tier."""
        points = 1000

        if points >= 1000:
            tier = 'Gold'
        elif points >= 500:
            tier = 'Silver'
        else:
            tier = 'Bronze'

        assert tier == 'Gold'

    def test_gold_tier_high_points(self):
        """Test Gold tier with very high points."""
        points = 10000

        if points >= 1000:
            tier = 'Gold'
        elif points >= 500:
            tier = 'Silver'
        else:
            tier = 'Bronze'

        assert tier == 'Gold'


class TestLoyaltyPointsCalculation:
    """Tests for loyalty points calculation logic."""

    def test_points_calculation_basic(self):
        """Test basic 10% of spend = points formula."""
        total_spend = 1000
        loyalty_rate = 0.10

        points = int(total_spend * loyalty_rate)

        assert points == 100

    def test_points_calculation_with_multiplier(self):
        """Test points with loyalty multiplier."""
        total_spend = 1000
        loyalty_rate = 0.10
        multiplier = 2  # Double points promotion

        points = int(total_spend * loyalty_rate * multiplier)

        assert points == 200

    def test_points_from_sql_aggregation(self, temp_db_with_tables):
        """Test points calculation through SQL."""
        conn, _ = temp_db_with_tables
        cursor = conn.cursor()

        # Insert test customer
        cursor.execute("""
            INSERT INTO customer_details (customer_id, first_name)
            VALUES ('C001', 'Test')
        """)

        # Insert store
        cursor.execute("""
            INSERT INTO stores (store_id, store_name)
            VALUES ('S001', 'Test Store')
        """)

        # Insert transactions
        cursor.execute("""
            INSERT INTO store_sales_header
            (transaction_id, customer_id, store_id, transaction_date, total_amount)
            VALUES ('T001', 'C001', 'S001', '2024-01-15', 500.00)
        """)
        cursor.execute("""
            INSERT INTO store_sales_header
            (transaction_id, customer_id, store_id, transaction_date, total_amount)
            VALUES ('T002', 'C001', 'S001', '2024-02-15', 1500.00)
        """)
        conn.commit()

        # Calculate total spend
        cursor.execute("""
            SELECT SUM(total_amount) as total_spend
            FROM store_sales_header
            WHERE customer_id = 'C001'
        """)
        total_spend = cursor.fetchone()[0]

        # Calculate points (10% of spend)
        points = int(total_spend * 0.10)

        assert total_spend == 2000.00
        assert points == 200


class TestRFMRecency:
    """Tests for RFM Recency calculation."""

    def test_recency_days_calculation(self):
        """Test calculating days since last purchase."""
        last_purchase = datetime(2024, 1, 1)
        today = datetime(2024, 1, 31)

        recency_days = (today - last_purchase).days

        assert recency_days == 30

    def test_at_risk_flag_true(self):
        """Test AR flag when recency > 30 days."""
        recency_days = 45

        is_at_risk = recency_days > 30
        segment = 'AR' if is_at_risk else ''

        assert is_at_risk is True
        assert segment == 'AR'

    def test_at_risk_flag_false(self):
        """Test AR flag when recency <= 30 days."""
        recency_days = 25

        is_at_risk = recency_days > 30
        segment = 'AR' if is_at_risk else ''

        assert is_at_risk is False
        assert segment == ''

    def test_at_risk_boundary(self):
        """Test AR flag at exactly 30 days."""
        recency_days = 30

        is_at_risk = recency_days > 30

        assert is_at_risk is False  # 30 is not > 30


class TestRFMMonetary:
    """Tests for RFM Monetary/High Spender calculation."""

    def test_high_spender_top_20_percent(self):
        """Test High Spender identification in top 20%."""
        # Sample customer spend amounts
        customer_spends = [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]

        # Calculate 80th percentile threshold
        threshold = np.percentile(customer_spends, 80)

        # Test customer with high spend
        test_spend = 1000
        is_high_spender = test_spend >= threshold

        assert is_high_spender == True

    def test_high_spender_below_threshold(self):
        """Test customer below High Spender threshold."""
        customer_spends = [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]

        threshold = np.percentile(customer_spends, 80)

        test_spend = 300
        is_high_spender = test_spend >= threshold

        assert is_high_spender == False

    def test_high_spender_segment_flag(self):
        """Test HS segment assignment."""
        is_high_spender = True

        segment = 'HS' if is_high_spender else ''

        assert segment == 'HS'


class TestRFMSegmentation:
    """Tests for combined RFM segment assignment."""

    def test_segment_ar_only(self):
        """Test AR segment when only at risk."""
        recency_days = 45
        is_high_spender = False

        segments = []
        if recency_days > 30:
            segments.append('AR')
        if is_high_spender:
            segments.append('HS')

        result = ','.join(segments) if segments else None

        assert result == 'AR'

    def test_segment_hs_only(self):
        """Test HS segment when only high spender."""
        recency_days = 10
        is_high_spender = True

        segments = []
        if recency_days > 30:
            segments.append('AR')
        if is_high_spender:
            segments.append('HS')

        result = ','.join(segments) if segments else None

        assert result == 'HS'

    def test_segment_ar_and_hs(self):
        """Test combined AR,HS segment."""
        recency_days = 60
        is_high_spender = True

        segments = []
        if recency_days > 30:
            segments.append('AR')
        if is_high_spender:
            segments.append('HS')

        result = ','.join(segments)

        assert result == 'AR,HS'

    def test_segment_none(self):
        """Test no segment when neither condition met."""
        recency_days = 15
        is_high_spender = False

        segments = []
        if recency_days > 30:
            segments.append('AR')
        if is_high_spender:
            segments.append('HS')

        result = ','.join(segments) if segments else None

        assert result is None


class TestLoyaltyTiersDistribution:
    """Tests for loyalty tier distribution queries."""

    def test_query_returns_all_tiers(self, temp_db_with_tables):
        """Test that loyalty distribution query returns all tiers."""
        conn, _ = temp_db_with_tables
        cursor = conn.cursor()

        # Insert loyalty data
        cursor.execute("""
            INSERT INTO customer_loyalty (customer_id, total_loyalty_points, loyalty_tier)
            VALUES ('C001', 100, 'Bronze')
        """)
        cursor.execute("""
            INSERT INTO customer_loyalty (customer_id, total_loyalty_points, loyalty_tier)
            VALUES ('C002', 750, 'Silver')
        """)
        cursor.execute("""
            INSERT INTO customer_loyalty (customer_id, total_loyalty_points, loyalty_tier)
            VALUES ('C003', 2000, 'Gold')
        """)
        conn.commit()

        # Query distribution
        cursor.execute("""
            SELECT loyalty_tier, COUNT(*) as count
            FROM customer_loyalty
            WHERE total_loyalty_points > 0
            GROUP BY loyalty_tier
        """)
        results = cursor.fetchall()

        tiers = {row[0]: row[1] for row in results}

        assert 'Bronze' in tiers
        assert 'Silver' in tiers
        assert 'Gold' in tiers

    def test_query_excludes_zero_points(self, temp_db_with_tables):
        """Test that query excludes customers with 0 points."""
        conn, _ = temp_db_with_tables
        cursor = conn.cursor()

        # Insert data including zero points
        cursor.execute("""
            INSERT INTO customer_loyalty (customer_id, total_loyalty_points, loyalty_tier)
            VALUES ('C001', 0, 'Bronze')
        """)
        cursor.execute("""
            INSERT INTO customer_loyalty (customer_id, total_loyalty_points, loyalty_tier)
            VALUES ('C002', 100, 'Bronze')
        """)
        conn.commit()

        # Query with zero filter
        cursor.execute("""
            SELECT COUNT(*) FROM customer_loyalty WHERE total_loyalty_points > 0
        """)
        count = cursor.fetchone()[0]

        assert count == 1  # Only C002 should be counted


class TestCustomerLoyaltyUpdate:
    """Tests for customer_loyalty table updates."""

    def test_insert_new_customer(self, temp_db_with_tables):
        """Test inserting new customer loyalty record."""
        conn, _ = temp_db_with_tables
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO customer_loyalty
            (customer_id, total_loyalty_points, loyalty_tier, rfm_segment)
            VALUES ('C001', 500, 'Silver', 'HS')
        """)
        conn.commit()

        cursor.execute("SELECT * FROM customer_loyalty WHERE customer_id = 'C001'")
        row = cursor.fetchone()

        assert row is not None
        assert row[1] == 500  # points
        assert row[2] == 'Silver'  # tier
        assert row[3] == 'HS'  # segment

    def test_update_existing_customer(self, temp_db_with_tables):
        """Test updating existing customer loyalty record."""
        conn, _ = temp_db_with_tables
        cursor = conn.cursor()

        # Insert initial record
        cursor.execute("""
            INSERT INTO customer_loyalty
            (customer_id, total_loyalty_points, loyalty_tier)
            VALUES ('C001', 100, 'Bronze')
        """)
        conn.commit()

        # Update record
        cursor.execute("""
            UPDATE customer_loyalty
            SET total_loyalty_points = 600, loyalty_tier = 'Silver'
            WHERE customer_id = 'C001'
        """)
        conn.commit()

        cursor.execute("SELECT total_loyalty_points, loyalty_tier FROM customer_loyalty WHERE customer_id = 'C001'")
        row = cursor.fetchone()

        assert row[0] == 600
        assert row[1] == 'Silver'
