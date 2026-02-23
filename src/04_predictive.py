"""
04_predictive.py - Predictive Analytics for RetailPulse

This script performs three predictive analytics functions:
1. Next Month Spend Forecast: Predict customer spending based on 3-month average
2. Restock Flag: Identify products likely to run out of stock
3. Promotion Sensitivity: Classify customers by their response to promotions

Usage:
    python src/04_predictive.py
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
    """
    Establish a connection to the SQLite database.

    Returns:
        sqlite3.Connection: Database connection object

    Raises:
        DatabaseError: If connection fails
    """
    try:
        if not os.path.exists(DB_PATH):
            raise DatabaseError(
                "Database file not found. Run previous pipeline scripts first.",
                operation='connect'
            )

        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        logger.debug(f"Connected to database: {DB_PATH}")
        return conn
    except sqlite3.Error as e:
        logger.error(f"Failed to connect to database: {e}")
        raise DatabaseError(f"Database connection failed: {e}", operation='connect')


@handle_exceptions(logger=logger)
def predict_next_month_spend(conn):
    """
    Prediction 1: Forecast next month's spend for each customer.

    Logic:
    - Calculate monthly spend for each customer
    - Use average of last 3 months (or fewer if not available)
    - Save predictions to customer_predictions table

    Args:
        conn: SQLite database connection

    Raises:
        AnalyticsError: If prediction fails
    """
    logger.info("Starting next month spend prediction")
    print("\n" + "=" * 60)
    print("PREDICTION 1: NEXT MONTH SPEND FORECAST")
    print("=" * 60)

    cursor = conn.cursor()
    today = datetime.now().strftime('%Y-%m-%d')

    # Get monthly spend per customer
    query = """
        SELECT
            customer_id,
            strftime('%Y-%m', transaction_date) AS month,
            SUM(total_amount) AS monthly_spend
        FROM store_sales_header
        WHERE customer_id IS NOT NULL AND customer_id != ''
        GROUP BY customer_id, month
        ORDER BY customer_id, month
    """

    try:
        df = pd.read_sql_query(query, conn)
    except Exception as e:
        logger.error(f"Failed to query transaction data: {e}")
        raise AnalyticsError(f"Failed to query transaction data: {e}", analysis_type='spend_forecast')

    if len(df) == 0:
        logger.warning("No transaction data found for spend prediction")
        print("No transaction data found for spend prediction")
        return

    # Calculate predictions for each customer
    predictions = []

    for customer_id in df['customer_id'].unique():
        customer_data = df[df['customer_id'] == customer_id].sort_values('month')

        # Use last 3 months (or fewer if not available)
        recent_spend = customer_data.tail(3)['monthly_spend']
        predicted_spend = recent_spend.mean()
        months_used = len(recent_spend)

        predictions.append({
            'customer_id': customer_id,
            'predicted_next_spend': round(predicted_spend, 2),
            'months_used': months_used
        })

    # Save to database
    for pred in predictions:
        cursor.execute("""
            INSERT OR REPLACE INTO customer_predictions
            (customer_id, predicted_next_spend, months_used, predicted_at)
            VALUES (?, ?, ?, ?)
        """, (pred['customer_id'], pred['predicted_next_spend'],
              pred['months_used'], today))

    conn.commit()

    print(f"Generated spend predictions for {len(predictions)} customers")

    # Statistics
    pred_df = pd.DataFrame(predictions)
    print(f"\nPrediction Statistics:")
    print(f"  Average predicted spend: ₹{pred_df['predicted_next_spend'].mean():,.2f}")
    print(f"  Min predicted spend: ₹{pred_df['predicted_next_spend'].min():,.2f}")
    print(f"  Max predicted spend: ₹{pred_df['predicted_next_spend'].max():,.2f}")

    # Distribution by months used
    months_dist = pred_df['months_used'].value_counts().sort_index()
    print(f"\nCustomers by historical data available:")
    for months, count in months_dist.items():
        print(f"  {months} month(s): {count} customers")

    logger.info(f"Spend prediction completed for {len(predictions)} customers")


@handle_exceptions(logger=logger)
def predict_restock_flag(conn):
    """
    Prediction 2: Identify products that need restocking.

    Logic:
    - Calculate average daily sales for each product over last 7 days
    - Project weekly demand
    - Flag products where projected demand > current stock

    Args:
        conn: SQLite database connection

    Raises:
        AnalyticsError: If restock prediction fails
    """
    logger.info("Starting restock flag prediction")
    print("\n" + "=" * 60)
    print("PREDICTION 2: RESTOCK FLAG")
    print("=" * 60)

    cursor = conn.cursor()

    # Calculate average daily sales over last 7 days
    # Note: Using the latest transaction date in the data as reference
    # since the data might not be from today

    # First, get the latest transaction date in the database
    cursor.execute("SELECT MAX(transaction_date) as latest FROM store_sales_header")
    result = cursor.fetchone()
    latest_date = result['latest'] if result else datetime.now().strftime('%Y-%m-%d')

    query = f"""
        SELECT
            li.product_id,
            SUM(li.quantity) * 1.0 / 7 AS avg_daily_sales
        FROM store_sales_line_items li
        JOIN store_sales_header h ON li.transaction_id = h.transaction_id
        WHERE h.transaction_date >= date('{latest_date}', '-7 days')
          AND li.quantity > 0
        GROUP BY li.product_id
    """

    sales_df = pd.read_sql_query(query, conn)

    # Get current stock levels
    stock_query = """
        SELECT product_id, product_name, current_stock_level
        FROM products
    """
    stock_df = pd.read_sql_query(stock_query, conn)

    # Merge sales with stock
    merged = stock_df.merge(sales_df, on='product_id', how='left')
    merged['avg_daily_sales'] = merged['avg_daily_sales'].fillna(0)
    merged['projected_weekly_demand'] = merged['avg_daily_sales'] * 7

    # Determine restock flag
    restock_count = 0
    safe_count = 0
    no_sales_count = 0

    for _, row in merged.iterrows():
        product_id = row['product_id']
        current_stock = row['current_stock_level'] or 0
        projected_demand = row['projected_weekly_demand']

        # Only update if there were sales in the last 7 days
        if projected_demand > 0:
            if projected_demand > current_stock:
                restock_flag = 1
                restock_count += 1
            else:
                restock_flag = 0
                safe_count += 1

            cursor.execute("""
                UPDATE products SET restock_flag = ? WHERE product_id = ?
            """, (restock_flag, product_id))
        else:
            no_sales_count += 1

    conn.commit()

    print(f"Analyzed {len(merged)} products")
    print(f"\nRestock Analysis:")
    print(f"  Products needing restock: {restock_count}")
    print(f"  Products with safe stock: {safe_count}")
    print(f"  Products with no recent sales (unchanged): {no_sales_count}")

    # Show products needing restock
    if restock_count > 0:
        cursor.execute("""
            SELECT product_name, current_stock_level
            FROM products
            WHERE restock_flag = 1
            ORDER BY current_stock_level ASC
            LIMIT 10
        """)
        restock_products = cursor.fetchall()

        print(f"\nTop products needing restock:")
        for p in restock_products:
            print(f"  - {p['product_name']}: {p['current_stock_level']} units left")

    logger.info(f"Restock prediction completed. {restock_count} products flagged.")


@handle_exceptions(logger=logger)
def predict_promotion_sensitivity(conn):
    """
    Prediction 3: Classify customers by promotion response rate.

    Logic:
    - Count total purchases and promotional purchases per customer
    - Calculate response rate = promo_purchases / total_purchases
    - Classify: HIGH (>50%), MEDIUM (20-50%), LOW (<20%)

    Args:
        conn: SQLite database connection

    Raises:
        AnalyticsError: If promotion sensitivity calculation fails
    """
    logger.info("Starting promotion sensitivity analysis")
    print("\n" + "=" * 60)
    print("PREDICTION 3: PROMOTION SENSITIVITY")
    print("=" * 60)

    cursor = conn.cursor()

    # Count total and promotional purchases per customer
    # Note: promotion_id is stored as TEXT like '1.0', '2.0', etc.
    query = """
        SELECT
            h.customer_id,
            COUNT(DISTINCT h.transaction_id) AS total_purchases,
            COUNT(DISTINCT CASE
                WHEN li.promotion_id IS NOT NULL
                AND li.promotion_id != ''
                THEN h.transaction_id
            END) AS promo_purchases
        FROM store_sales_header h
        LEFT JOIN store_sales_line_items li ON h.transaction_id = li.transaction_id
        WHERE h.customer_id IS NOT NULL AND h.customer_id != ''
        GROUP BY h.customer_id
    """

    df = pd.read_sql_query(query, conn)

    if len(df) == 0:
        print("No customer purchase data found")
        return

    # Calculate sensitivity and update
    high_count = 0
    medium_count = 0
    low_count = 0

    for _, row in df.iterrows():
        customer_id = row['customer_id']
        total = row['total_purchases']
        promo = row['promo_purchases']

        if total > 0:
            response_rate = promo / total

            if response_rate > 0.50:
                sensitivity = 'HIGH'
                high_count += 1
            elif response_rate >= 0.20:
                sensitivity = 'MEDIUM'
                medium_count += 1
            else:
                sensitivity = 'LOW'
                low_count += 1

            cursor.execute("""
                UPDATE customer_details
                SET promotion_sensitivity = ?
                WHERE customer_id = ?
            """, (sensitivity, customer_id))

    conn.commit()

    total_customers = high_count + medium_count + low_count
    print(f"Analyzed {total_customers} customers")

    print(f"\nPromotion Sensitivity Distribution:")
    print(f"  HIGH (>50% promo response):   {high_count} customers ({100*high_count/total_customers:.1f}%)")
    print(f"  MEDIUM (20-50% response):     {medium_count} customers ({100*medium_count/total_customers:.1f}%)")
    print(f"  LOW (<20% response):          {low_count} customers ({100*low_count/total_customers:.1f}%)")

    # Show some statistics
    df['response_rate'] = df['promo_purchases'] / df['total_purchases']
    df['response_rate'] = df['response_rate'].fillna(0)

    print(f"\nResponse Rate Statistics:")
    print(f"  Average response rate: {df['response_rate'].mean()*100:.1f}%")
    print(f"  Median response rate: {df['response_rate'].median()*100:.1f}%")

    logger.info("Promotion sensitivity analysis completed")


def main():
    """Main entry point for predictive analytics."""
    logger.info("=" * 60)
    logger.info("RetailPulse - Predictive Analytics Engine Started")
    print("=" * 60)
    print("RetailPulse - Predictive Analytics Engine")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    conn = None
    exit_code = 0
    prediction_errors = []

    try:
        conn = get_connection()
        logger.info(f"Connected to database: {DB_PATH}")
        print(f"Connected to database: {DB_PATH}")

        # Run all predictions with individual error handling
        try:
            predict_next_month_spend(conn)
        except AnalyticsError as e:
            logger.error(f"Spend prediction error: {e}")
            prediction_errors.append(f"Spend prediction: {e}")

        try:
            predict_restock_flag(conn)
        except AnalyticsError as e:
            logger.error(f"Restock prediction error: {e}")
            prediction_errors.append(f"Restock prediction: {e}")

        try:
            predict_promotion_sensitivity(conn)
        except AnalyticsError as e:
            logger.error(f"Promotion sensitivity error: {e}")
            prediction_errors.append(f"Promotion sensitivity: {e}")

        print("\n" + "=" * 60)
        if prediction_errors:
            print(f"Predictive analytics completed with {len(prediction_errors)} warning(s)")
            for err in prediction_errors:
                print(f"  - {err}")
            exit_code = 1
        else:
            print("Predictive analytics completed successfully!")
        print("=" * 60)
        logger.info("Predictive analytics engine completed")

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
