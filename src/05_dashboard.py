"""
05_dashboard.py - Visualization Dashboard for RetailPulse

This script generates a comprehensive dashboard with 4 charts:
1. Total Sales by Store (vertical bar chart)
2. Top 10 Selling Products by Quantity (horizontal bar chart)
3. Loyalty Points Distribution (histogram by tier)
4. At-Risk Customers per Store (bonus chart for innovation points)

Output:
- Combined dashboard image: output/dashboard.png
- Individual chart images: output/chart1_*.png, output/chart2_*.png, etc.

Usage:
    python src/05_dashboard.py
"""

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import os
import sys
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(__file__))

from utils.error_handler import (
    get_logger,
    DatabaseError,
    FileError,
    handle_exceptions,
    validate_directory_exists,
)

# Initialize logger
logger = get_logger(__name__)

# Database path configuration
DB_PATH = os.path.join(os.path.dirname(__file__), "../db/retail.db")
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "../output")


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


def format_rupees(x, pos):
    """Format axis labels with Rupee symbol."""
    if x >= 1e7:
        return f'₹{x/1e7:.1f}Cr'
    elif x >= 1e5:
        return f'₹{x/1e5:.1f}L'
    elif x >= 1e3:
        return f'₹{x/1e3:.0f}K'
    else:
        return f'₹{x:.0f}'


@handle_exceptions(logger=logger)
def chart1_sales_by_store(ax, conn):
    """
    Chart 1: Total Sales by Store (Vertical Bar Chart)

    Args:
        ax: Matplotlib axes object
        conn: Database connection
    """
    query = """
        SELECT s.store_name, SUM(h.total_amount) AS total_sales
        FROM store_sales_header h
        JOIN stores s ON h.store_id = s.store_id
        GROUP BY s.store_name
        ORDER BY total_sales DESC
    """

    df = pd.read_sql_query(query, conn)

    if len(df) == 0:
        ax.text(0.5, 0.5, 'No data available', ha='center', va='center')
        ax.set_title('Total Sales by Store')
        return

    # Create bar chart
    bars = ax.bar(df['store_name'], df['total_sales'], color='#2E75B6', edgecolor='white')

    # Formatting
    ax.set_title('Total Sales by Store', fontsize=12, fontweight='bold', pad=10)
    ax.set_xlabel('Store', fontsize=10)
    ax.set_ylabel('Total Sales', fontsize=10)
    ax.tick_params(axis='x', rotation=45)

    # Format y-axis with Rupee symbol
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(format_rupees))

    # Add value labels on bars
    for bar, value in zip(bars, df['total_sales']):
        height = bar.get_height()
        ax.annotate(f'₹{value/1e5:.1f}L',
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha='center', va='bottom', fontsize=8)

    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)


@handle_exceptions(logger=logger)
def chart2_top_products(ax, conn):
    """
    Chart 2: Top 10 Selling Products by Quantity (Horizontal Bar Chart)

    Args:
        ax: Matplotlib axes object
        conn: Database connection
    """
    query = """
        SELECT p.product_name, p.product_category, SUM(li.quantity) AS total_qty
        FROM store_sales_line_items li
        JOIN products p ON li.product_id = p.product_id
        WHERE li.quantity > 0
        GROUP BY p.product_name, p.product_category
        ORDER BY total_qty DESC
        LIMIT 10
    """

    df = pd.read_sql_query(query, conn)

    if len(df) == 0:
        ax.text(0.5, 0.5, 'No data available', ha='center', va='center')
        ax.set_title('Top 10 Products by Quantity Sold')
        return

    # Create display name with category
    df['display_name'] = df['product_name'] + '\n(' + df['product_category'] + ')'

    # Color by category
    category_colors = {
        'Electronics': '#636EFA',
        'Apparel': '#EF553B',
        'Grocery': '#00CC96',
        'Home': '#AB63FA'
    }
    colors = [category_colors.get(cat, '#00B050') for cat in df['product_category']]

    # Create horizontal bar chart
    bars = ax.barh(df['display_name'], df['total_qty'], color=colors, edgecolor='white')

    # Formatting
    ax.set_title('Top 10 Products by Quantity Sold', fontsize=12, fontweight='bold', pad=10)
    ax.set_xlabel('Quantity Sold', fontsize=10)
    ax.set_ylabel('Product (Category)', fontsize=10)

    # Invert y-axis so highest is at top
    ax.invert_yaxis()

    # Add value labels
    for bar, value in zip(bars, df['total_qty']):
        width = bar.get_width()
        ax.annotate(f'{int(value):,}',
                    xy=(width, bar.get_y() + bar.get_height() / 2),
                    xytext=(3, 0),
                    textcoords="offset points",
                    ha='left', va='center', fontsize=8)

    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)


@handle_exceptions(logger=logger)
def chart3_loyalty_distribution(ax, conn):
    """
    Chart 3: Loyalty Points Distribution (Histogram by Tier)

    Args:
        ax: Matplotlib axes object
        conn: Database connection
    """
    query = """
        SELECT total_loyalty_points, loyalty_status
        FROM customer_details
        WHERE total_loyalty_points IS NOT NULL AND total_loyalty_points > 0
    """

    df = pd.read_sql_query(query, conn)

    if len(df) == 0:
        ax.text(0.5, 0.5, 'No data available', ha='center', va='center')
        ax.set_title('Loyalty Points Distribution')
        return

    # Color mapping for tiers
    colors = {
        'Bronze': '#CD7F32',
        'Silver': '#C0C0C0',
        'Gold': '#FFD700'
    }

    # Plot overlapping histograms for each tier
    for tier in ['Bronze', 'Silver', 'Gold']:
        tier_data = df[df['loyalty_status'] == tier]['total_loyalty_points']
        if len(tier_data) > 0:
            ax.hist(tier_data, bins=20, alpha=0.6, label=tier,
                    color=colors.get(tier, 'gray'), edgecolor='white')

    # Formatting
    ax.set_title('Loyalty Points Distribution', fontsize=12, fontweight='bold', pad=10)
    ax.set_xlabel('Loyalty Points', fontsize=10)
    ax.set_ylabel('Number of Customers', fontsize=10)
    ax.legend(title='Loyalty Tier', loc='upper right')

    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)


@handle_exceptions(logger=logger)
def chart4_at_risk_by_store(ax, conn):
    """
    Chart 4 (BONUS): At-Risk Customers per Store

    This chart shows the count of customers in the 'AR' (At Risk) segment
    for each store, helping identify stores with customer retention issues.

    Args:
        ax: Matplotlib axes object
        conn: Database connection
    """
    query = """
        SELECT s.store_name, COUNT(DISTINCT h.customer_id) AS at_risk_count
        FROM store_sales_header h
        JOIN stores s ON h.store_id = s.store_id
        JOIN customer_details c ON h.customer_id = c.customer_id
        WHERE c.segment_id = 'AR'
        GROUP BY s.store_name
        ORDER BY at_risk_count DESC
    """

    df = pd.read_sql_query(query, conn)

    if len(df) == 0:
        ax.text(0.5, 0.5, 'No at-risk customers found', ha='center', va='center')
        ax.set_title('At-Risk Customers by Store')
        return

    # Create bar chart with warning color
    bars = ax.bar(df['store_name'], df['at_risk_count'], color='#FF6B6B', edgecolor='white')

    # Formatting
    ax.set_title('At-Risk Customers by Store', fontsize=12, fontweight='bold', pad=10)
    ax.set_xlabel('Store', fontsize=10)
    ax.set_ylabel('At-Risk Customer Count', fontsize=10)
    ax.tick_params(axis='x', rotation=45)

    # Add value labels
    for bar, value in zip(bars, df['at_risk_count']):
        height = bar.get_height()
        ax.annotate(f'{int(value)}',
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha='center', va='bottom', fontsize=9, fontweight='bold')

    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)


@handle_exceptions(logger=logger)
def save_individual_chart(chart_func, filename, conn, figsize=(8, 6)):
    """
    Save an individual chart as a standalone image.

    Args:
        chart_func: Function to create the chart
        filename: Output filename
        conn: Database connection
        figsize: Figure size tuple

    Raises:
        FileError: If chart cannot be saved
    """
    try:
        fig, ax = plt.subplots(figsize=figsize)
        chart_func(ax, conn)
        plt.tight_layout()
        filepath = os.path.join(OUTPUT_PATH, filename)
        plt.savefig(filepath, dpi=150, bbox_inches='tight', facecolor='white')
        plt.close()
        logger.info(f"Saved individual chart: {filename}")
        print(f"  Saved: {filename}")
    except Exception as e:
        logger.error(f"Failed to save chart {filename}: {e}")
        plt.close()
        raise FileError(f"Failed to save chart: {e}", filepath=filename)


def main():
    """
    Main entry point for dashboard generation.

    Returns:
        int: Exit code (0 for success, 1 for error)
    """
    logger.info("Starting RetailPulse Dashboard Generator")
    print("=" * 60)
    print("RetailPulse - Dashboard Generator")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Check if database exists
    if not os.path.exists(DB_PATH):
        error_msg = f"Database not found at {DB_PATH}. Run pipeline scripts first."
        logger.error(error_msg)
        print(f"ERROR: {error_msg}")
        return 1

    # Ensure output directory exists
    try:
        validate_directory_exists(os.path.dirname(OUTPUT_PATH), create=True)
        os.makedirs(OUTPUT_PATH, exist_ok=True)
        logger.debug(f"Output directory ready: {OUTPUT_PATH}")
    except Exception as e:
        logger.error(f"Failed to create output directory: {e}")
        print(f"ERROR: Failed to create output directory: {e}")
        return 1

    conn = None
    chart_errors = []

    try:
        conn = get_connection()
        print(f"Connected to database: {DB_PATH}")
        logger.info(f"Connected to database: {DB_PATH}")

        # Create combined dashboard with 2x2 grid
        print("\nGenerating combined dashboard...")
        logger.info("Generating combined dashboard")

        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('RetailPulse — Retail Analytics Dashboard',
                     fontsize=16, fontweight='bold', y=0.98)

        # Generate all charts with individual error handling
        chart_functions = [
            (chart1_sales_by_store, axes[0, 0], "Sales by Store"),
            (chart2_top_products, axes[0, 1], "Top Products"),
            (chart3_loyalty_distribution, axes[1, 0], "Loyalty Distribution"),
            (chart4_at_risk_by_store, axes[1, 1], "At-Risk Customers"),
        ]

        for chart_func, ax, chart_name in chart_functions:
            try:
                chart_func(ax, conn)
                logger.debug(f"Generated chart: {chart_name}")
            except Exception as e:
                logger.error(f"Failed to generate chart '{chart_name}': {e}")
                chart_errors.append(chart_name)
                ax.text(0.5, 0.5, f'Error: {chart_name}', ha='center', va='center', color='red')
                ax.set_title(f'{chart_name} (Error)')

        # Adjust layout and save
        plt.tight_layout(rect=[0, 0, 1, 0.96])
        dashboard_path = os.path.join(OUTPUT_PATH, "dashboard.png")

        try:
            plt.savefig(dashboard_path, dpi=150, bbox_inches='tight', facecolor='white')
            logger.info(f"Saved combined dashboard: {dashboard_path}")
            print(f"  Saved: dashboard.png")
        except Exception as e:
            logger.error(f"Failed to save combined dashboard: {e}")
            chart_errors.append("Combined Dashboard")
        finally:
            plt.close()

        # Save individual charts
        print("\nGenerating individual charts...")
        logger.info("Generating individual charts")

        individual_charts = [
            (chart1_sales_by_store, "chart1_sales_by_store.png"),
            (chart2_top_products, "chart2_top_products.png"),
            (chart3_loyalty_distribution, "chart3_loyalty_distribution.png"),
            (chart4_at_risk_by_store, "chart4_at_risk_customers.png"),
        ]

        for chart_func, filename in individual_charts:
            try:
                save_individual_chart(chart_func, filename, conn)
            except Exception as e:
                logger.error(f"Failed to save individual chart {filename}: {e}")
                chart_errors.append(filename)

        # Summary
        print("\n" + "=" * 60)
        if chart_errors:
            logger.warning(f"Dashboard generation completed with {len(chart_errors)} error(s)")
            print(f"Dashboard generation completed with {len(chart_errors)} error(s):")
            for err in chart_errors:
                print(f"  - {err}")
        else:
            logger.info("Dashboard generation completed successfully")
            print("Dashboard generation completed successfully!")
        print(f"Output saved to: {OUTPUT_PATH}")
        print("=" * 60)

        return 0 if not chart_errors else 1

    except DatabaseError as e:
        logger.error(f"Database error: {e}")
        print(f"DATABASE ERROR: {e}")
        return 1
    except FileError as e:
        logger.error(f"File error: {e}")
        print(f"FILE ERROR: {e}")
        return 1
    except Exception as e:
        logger.exception(f"Unexpected error during dashboard generation: {e}")
        print(f"ERROR: {e}")
        return 1
    finally:
        if conn:
            conn.close()
            logger.debug("Database connection closed")
            print("\nDatabase connection closed.")


if __name__ == "__main__":
    sys.exit(main())
