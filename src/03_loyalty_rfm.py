"""
03_loyalty_rfm.py - Loyalty Points & RFM Segmentation for RetailPulse

This script performs two main functions:
1. PART A: Calculate and update loyalty points for each customer based on active rules
2. PART B: Calculate RFM (Recency, Frequency, Monetary) scores and segment customers

Segments:
- HS (High Spender): Top 20% of customers by monetary value
- AR (At Risk): Customers with recency > 30 days
- If both apply, HS takes priority

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
    AnalyticsError,
    handle_exceptions,
)

# Initialize logger
logger = get_logger(__name__)

# Database path configuration
DB_PATH = os.path.join(os.path.dirname(__file__), "../db/retail.db")


@handle_exceptions(logger=logger)
def get_connection():
    try:
        if not os.path.exists(DB_PATH):
            raise DatabaseError(
                "Database file not found. Run 01_setup_db.py and 02_etl_pipeline.py first.",
                operation='connect'
            )

        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        logger.debug(f"Connected to database: {DB_PATH}")
        return conn
    except sqlite3.Error as e:
        logger.error(f"Failed to connect to database: {e}")
        raise DatabaseError(f"Database connection failed: {e}", operation='connect')


def get_loyalty_status(points):
    if points >= 1000:
        return 'Gold'
    elif points >= 500:
        return 'Silver'
    else:
        return 'Bronze'


@handle_exceptions(logger=logger)
def calculate_loyalty_points(conn):
    logger.info("Starting loyalty points calculation")
    print("\n" + "=" * 60)
    print("PART A: LOYALTY POINTS CALCULATION")
    print("=" * 60)

    cursor = conn.cursor()
    today = datetime.now().strftime('%Y-%m-%d')

    # Step 1: Get active loyalty rules
    # A rule is active if today falls between start_date and end_date
    # or if dates are null (treat as always active)
    rules_query = """
        SELECT rule_id, rule_name, points_per_unit_spend,
               min_spend_threshold, bonus_points
        FROM loyalty_rules
        WHERE (start_date IS NULL OR start_date <= ?)
          AND (end_date IS NULL OR end_date >= ?)
    """
    cursor.execute(rules_query, (today, today))
    rules = cursor.fetchall()

    if not rules:
        print("No active loyalty rules found. Using default multiplier.")
        rules = [{'points_per_unit_spend': 0.01, 'min_spend_threshold': 1000, 'bonus_points': 50}]
    else:
        rules = [dict(r) for r in rules]

    print(f"Found {len(rules)} active loyalty rule(s)")

    # Step 2: Calculate total spend per customer
    spend_query = """
        SELECT customer_id, SUM(total_amount) as total_spend
        FROM store_sales_header
        WHERE customer_id IS NOT NULL AND customer_id != ''
        GROUP BY customer_id
    """
    spend_df = pd.read_sql_query(spend_query, conn)

    print(f"Calculating points for {len(spend_df)} customers...")

    # Step 3 & 4: Apply rules and calculate points
    updated_count = 0
    for _, row in spend_df.iterrows():
        customer_id = row['customer_id']
        total_spend = row['total_spend'] or 0

        total_points = 0
        for rule in rules:
            # Base points calculation
            points_per_unit = rule.get('points_per_unit_spend', 0.01)
            points = (total_spend / 1000) * (points_per_unit * 1000)  # Adjust formula

            # Bonus points if threshold met
            min_threshold = rule.get('min_spend_threshold', 0)
            bonus = rule.get('bonus_points', 0)
            if min_threshold and total_spend >= min_threshold:
                points += bonus

            total_points += points

        total_points = int(total_points)

        # Step 5: Determine tier and update
        status = get_loyalty_status(total_points)

        cursor.execute("""
            UPDATE customer_details
            SET total_loyalty_points = ?, loyalty_status = ?
            WHERE customer_id = ?
        """, (total_points, status, customer_id))

        updated_count += 1

    conn.commit()
    print(f"Updated loyalty points for {updated_count} customers")

    # Print tier distribution
    cursor.execute("""
        SELECT loyalty_status, COUNT(*) as count
        FROM customer_details
        WHERE loyalty_status IS NOT NULL
        GROUP BY loyalty_status
        ORDER BY count DESC
    """)
    tiers = cursor.fetchall()

    print("\nLoyalty Tier Distribution:")
    for tier in tiers:
        print(f"  {tier['loyalty_status']}: {tier['count']} customers")

    logger.info(f"Loyalty points calculation completed for {updated_count} customers")


@handle_exceptions(logger=logger)
def calculate_rfm(conn):
    logger.info("Starting RFM segmentation")
    print("\n" + "=" * 60)
    print("PART B: RFM SEGMENTATION")
    print("=" * 60)

    cursor = conn.cursor()
    today = datetime.now().strftime('%Y-%m-%d')

    # Step 1: Calculate RFM metrics for each customer
    rfm_query = """
        SELECT
            customer_id,
            CAST(julianday('now') - julianday(MAX(transaction_date)) AS INTEGER) AS recency_days,
            COUNT(DISTINCT transaction_id) AS frequency,
            SUM(total_amount) AS monetary_value
        FROM store_sales_header
        WHERE customer_id IS NOT NULL AND customer_id != ''
        GROUP BY customer_id
    """

    rfm_df = pd.read_sql_query(rfm_query, conn)

    if len(rfm_df) == 0:
        print("No transaction data found for RFM calculation")
        return

    print(f"Calculated RFM for {len(rfm_df)} customers")

    # Step 2: Save RFM to rfm_summary table
    print("Saving RFM summary to database...")

    for _, row in rfm_df.iterrows():
        cursor.execute("""
            INSERT OR REPLACE INTO rfm_summary
            (customer_id, recency_days, frequency, monetary_value, calculated_at)
            VALUES (?, ?, ?, ?, ?)
        """, (row['customer_id'], row['recency_days'], row['frequency'],
              row['monetary_value'], today))

    conn.commit()

    # Step 3: Determine segments
    # HS: Top 20% by monetary value
    hs_threshold = rfm_df['monetary_value'].quantile(0.80)
    hs_customers = set(rfm_df[rfm_df['monetary_value'] >= hs_threshold]['customer_id'].tolist())

    # AR: Recency > 30 days
    ar_customers = set(rfm_df[rfm_df['recency_days'] > 30]['customer_id'].tolist())

    print(f"\nSegmentation Thresholds:")
    print(f"  HS threshold (top 20%): ₹{hs_threshold:,.2f}")
    print(f"  AR threshold: > 30 days recency")

    print(f"\nSegment Counts:")
    print(f"  High Spenders (HS): {len(hs_customers)} customers")
    print(f"  At Risk (AR): {len(ar_customers)} customers")
    print(f"  Overlap (both HS and AR): {len(hs_customers & ar_customers)} customers")

    # Step 4: Update customer_details with segments
    # HS takes priority over AR
    updated_hs = 0
    updated_ar = 0

    for customer_id in hs_customers:
        cursor.execute("""
            UPDATE customer_details SET segment_id = 'HS' WHERE customer_id = ?
        """, (customer_id,))
        updated_hs += 1

    # AR gets assigned only if not already HS
    for customer_id in ar_customers:
        if customer_id not in hs_customers:
            cursor.execute("""
                UPDATE customer_details SET segment_id = 'AR' WHERE customer_id = ?
            """, (customer_id,))
            updated_ar += 1

    # Clear segment for customers who don't qualify for either
    all_segmented = hs_customers | ar_customers
    cursor.execute("""
        UPDATE customer_details SET segment_id = NULL
        WHERE customer_id NOT IN ({})
    """.format(','.join(['?' for _ in all_segmented])), tuple(all_segmented))

    conn.commit()

    print(f"\nSegments Assigned:")
    print(f"  HS assigned: {updated_hs} customers")
    print(f"  AR assigned: {updated_ar} customers")

    # Update last_purchase_date for customers
    cursor.execute("""
        UPDATE customer_details
        SET last_purchase_date = (
            SELECT MAX(transaction_date)
            FROM store_sales_header
            WHERE store_sales_header.customer_id = customer_details.customer_id
        )
        WHERE customer_id IN (
            SELECT DISTINCT customer_id FROM store_sales_header
            WHERE customer_id IS NOT NULL
        )
    """)
    conn.commit()

    print("\nUpdated last_purchase_date for all customers with transactions")

    # Print RFM statistics
    print("\nRFM Statistics:")
    print(f"  Recency  - Min: {rfm_df['recency_days'].min()} days, "
          f"Max: {rfm_df['recency_days'].max()} days, "
          f"Avg: {rfm_df['recency_days'].mean():.1f} days")
    print(f"  Frequency - Min: {rfm_df['frequency'].min()}, "
          f"Max: {rfm_df['frequency'].max()}, "
          f"Avg: {rfm_df['frequency'].mean():.1f}")
    print(f"  Monetary  - Min: ₹{rfm_df['monetary_value'].min():,.2f}, "
          f"Max: ₹{rfm_df['monetary_value'].max():,.2f}, "
          f"Avg: ₹{rfm_df['monetary_value'].mean():,.2f}")

    logger.info("RFM segmentation completed")


def main():
    """Main entry point for loyalty and RFM calculations."""
    logger.info("=" * 60)
    logger.info("RetailPulse - Loyalty & RFM Engine Started")
    print("=" * 60)
    print("RetailPulse - Loyalty & RFM Engine")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    conn = None
    exit_code = 0

    try:
        conn = get_connection()
        logger.info(f"Connected to database: {DB_PATH}")
        print(f"Connected to database: {DB_PATH}")

        # Part A: Calculate loyalty points
        try:
            calculate_loyalty_points(conn)
        except AnalyticsError as e:
            logger.error(f"Loyalty calculation error: {e}")
            print(f"Warning: Loyalty calculation issue: {e}")

        # Part B: Calculate RFM and segmentation
        try:
            calculate_rfm(conn)
        except AnalyticsError as e:
            logger.error(f"RFM calculation error: {e}")
            print(f"Warning: RFM calculation issue: {e}")

        print("\n" + "=" * 60)
        print("Loyalty & RFM calculation completed successfully!")
        print("=" * 60)
        logger.info("Loyalty & RFM engine completed successfully")

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

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
