"""
05_dashboard_streamlit.py - Interactive Dashboard for RetailPulse

This script generates an interactive Streamlit dashboard with 4 charts:
1. Total Sales by Store (vertical bar chart)
2. Top 10 Selling Products by Quantity with Category (horizontal bar chart)
3. Loyalty Points Distribution (histogram by tier - Bronze, Silver, Gold)
4. At-Risk Customers per Store (bonus chart for innovation points)

Usage:
    streamlit run src/05_dashboard_streamlit.py
"""

import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import os
import sys
import logging

# Configure logging for Streamlit
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database path configuration
DB_PATH = os.path.join(os.path.dirname(__file__), "../db/retail.db")


def check_database_exists():
    """Check if database file exists and display error if not."""
    if not os.path.exists(DB_PATH):
        st.error("**Database not found!**")
        st.warning(f"Database path: `{DB_PATH}`")
        st.info("""
        Please run the following scripts first:
        1. `python src/01_setup_db.py`
        2. `python src/02_etl_pipeline.py`
        3. `python src/03_loyalty_rfm.py`
        4. `python src/04_predictive.py`
        """)
        return False
    return True


def get_connection():
    """
    Establish a connection to the SQLite database.

    Returns:
        sqlite3.Connection: Database connection object or None if error
    """
    try:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error as e:
        logger.error(f"Database connection error: {e}")
        st.error(f"Database connection error: {e}")
        return None


def safe_query(query, error_message="Query failed"):
    """
    Safely execute a query and return DataFrame.

    Args:
        query: SQL query string
        error_message: Message to display if query fails

    Returns:
        DataFrame or empty DataFrame if error
    """
    try:
        conn = get_connection()
        if conn is None:
            return pd.DataFrame()
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except sqlite3.Error as e:
        logger.error(f"SQL error: {e}")
        st.warning(f"{error_message}: {e}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Query error: {e}")
        st.warning(f"{error_message}: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_sales_by_store():
    """Load total sales by store data."""
    query = """
        SELECT s.store_name, s.store_city, s.store_region,
               SUM(h.total_amount) AS total_sales,
               COUNT(DISTINCT h.transaction_id) AS transaction_count
        FROM store_sales_header h
        JOIN stores s ON h.store_id = s.store_id
        GROUP BY s.store_name, s.store_city, s.store_region
        ORDER BY total_sales DESC
    """
    return safe_query(query, "Failed to load sales data")


@st.cache_data(ttl=300)
def load_top_products():
    """Load top products by quantity with category."""
    query = """
        SELECT p.product_name, p.product_category, SUM(li.quantity) AS total_qty,
               SUM(li.line_item_amount) AS total_revenue
        FROM store_sales_line_items li
        JOIN products p ON li.product_id = p.product_id
        WHERE li.quantity > 0
        GROUP BY p.product_name, p.product_category
        ORDER BY total_qty DESC
        LIMIT 10
    """
    df = safe_query(query, "Failed to load products data")
    if not df.empty:
        # Create display name with category
        df['display_name'] = df['product_name'] + ' (' + df['product_category'].fillna('Unknown') + ')'
    return df


@st.cache_data(ttl=300)
def load_loyalty_distribution():
    """Load loyalty points distribution by tier."""
    query = """
        SELECT customer_id, total_loyalty_points, loyalty_status
        FROM customer_details
        WHERE total_loyalty_points IS NOT NULL AND total_loyalty_points > 0
    """
    return safe_query(query, "Failed to load loyalty data")


@st.cache_data(ttl=300)
def load_at_risk_customers():
    """Load at-risk customers per store."""
    query = """
        SELECT s.store_name, s.store_city,
               COUNT(DISTINCT h.customer_id) AS at_risk_count
        FROM store_sales_header h
        JOIN stores s ON h.store_id = s.store_id
        JOIN customer_details c ON h.customer_id = c.customer_id
        WHERE c.segment_id = 'AR'
        GROUP BY s.store_name, s.store_city
        ORDER BY at_risk_count DESC
    """
    return safe_query(query, "Failed to load at-risk customers data")


@st.cache_data(ttl=300)
def load_summary_metrics():
    """Load summary metrics for the dashboard."""
    try:
        conn = get_connection()
        if conn is None:
            return {'total_sales': 0, 'total_customers': 0, 'total_transactions': 0, 'restock_needed': 0}

        cursor = conn.cursor()

        # Total sales
        cursor.execute("SELECT COALESCE(SUM(total_amount), 0) FROM store_sales_header")
        total_sales = cursor.fetchone()[0] or 0

        # Total customers
        cursor.execute("SELECT COUNT(*) FROM customer_details")
        total_customers = cursor.fetchone()[0] or 0

        # Total transactions
        cursor.execute("SELECT COUNT(*) FROM store_sales_header")
        total_transactions = cursor.fetchone()[0] or 0

        # Products needing restock
        cursor.execute("SELECT COUNT(*) FROM products WHERE restock_flag = 1")
        restock_needed = cursor.fetchone()[0] or 0

        # Loyalty tier counts
        cursor.execute("""
            SELECT loyalty_status, COUNT(*) as count
            FROM customer_details
            WHERE loyalty_status IS NOT NULL
            GROUP BY loyalty_status
        """)
        tier_counts = {row[0]: row[1] for row in cursor.fetchall()}

        conn.close()

        return {
            'total_sales': total_sales,
            'total_customers': total_customers,
            'total_transactions': total_transactions,
            'restock_needed': restock_needed,
            'tier_counts': tier_counts
        }
    except Exception as e:
        logger.error(f"Error loading summary metrics: {e}")
        return {
            'total_sales': 0,
            'total_customers': 0,
            'total_transactions': 0,
            'restock_needed': 0,
            'tier_counts': {}
        }


def chart_sales_by_store(df):
    """Create interactive sales by store chart."""
    fig = px.bar(
        df,
        x='store_name',
        y='total_sales',
        color='store_region',
        title='Total Sales by Store',
        labels={'store_name': 'Store', 'total_sales': 'Total Sales (₹)', 'store_region': 'Region'},
        hover_data=['store_city', 'transaction_count'],
        color_discrete_sequence=px.colors.qualitative.Set2
    )

    fig.update_layout(
        xaxis_tickangle=-45,
        yaxis_tickformat=',.0f',
        showlegend=True,
        legend_title_text='Region',
        hovermode='x unified'
    )

    # Add value labels on bars
    fig.update_traces(
        texttemplate='₹%{y:,.0f}',
        textposition='outside',
        textfont_size=10
    )

    return fig


def chart_top_products(df):
    """Create interactive top products chart with category."""
    fig = px.bar(
        df,
        y='display_name',
        x='total_qty',
        color='product_category',
        title='Top 10 Products by Quantity Sold',
        labels={'display_name': 'Product (Category)', 'total_qty': 'Quantity Sold',
                'product_category': 'Category'},
        orientation='h',
        hover_data=['total_revenue'],
        color_discrete_map={
            'Electronics': '#636EFA',
            'Apparel': '#EF553B',
            'Grocery': '#00CC96',
            'Home': '#AB63FA'
        }
    )

    fig.update_layout(
        yaxis={'categoryorder': 'total ascending'},
        showlegend=True,
        legend_title_text='Category',
        hovermode='y unified'
    )

    # Add value labels
    fig.update_traces(
        texttemplate='%{x:,}',
        textposition='outside',
        textfont_size=10
    )

    return fig


def chart_loyalty_distribution(df):
    """Create interactive loyalty points distribution histogram by tier."""
    # Define tier colors
    tier_colors = {
        'Bronze': '#CD7F32',
        'Silver': '#C0C0C0',
        'Gold': '#FFD700'
    }

    # Create figure with overlapping histograms
    fig = go.Figure()

    # Sort tiers for consistent layering (Bronze at back, Gold at front)
    for tier in ['Bronze', 'Silver', 'Gold']:
        tier_data = df[df['loyalty_status'] == tier]['total_loyalty_points']
        if len(tier_data) > 0:
            fig.add_trace(go.Histogram(
                x=tier_data,
                name=tier,
                marker_color=tier_colors[tier],
                opacity=0.7,
                nbinsx=20,
                hovertemplate=f'{tier}<br>Points: %{{x}}<br>Count: %{{y}}<extra></extra>'
            ))

    fig.update_layout(
        title='Loyalty Points Distribution by Tier',
        xaxis_title='Loyalty Points',
        yaxis_title='Number of Customers',
        barmode='overlay',
        legend_title_text='Loyalty Tier',
        hovermode='x unified'
    )

    # Add tier threshold lines
    fig.add_vline(x=500, line_dash="dash", line_color="gray",
                  annotation_text="Silver Threshold (500)", annotation_position="top")
    fig.add_vline(x=1000, line_dash="dash", line_color="gray",
                  annotation_text="Gold Threshold (1000)", annotation_position="top")

    return fig


def chart_at_risk_customers(df):
    """Create interactive at-risk customers chart."""
    fig = px.bar(
        df,
        x='store_name',
        y='at_risk_count',
        color='at_risk_count',
        title='At-Risk Customers by Store (>30 days since last purchase)',
        labels={'store_name': 'Store', 'at_risk_count': 'At-Risk Customer Count'},
        hover_data=['store_city'],
        color_continuous_scale='Reds'
    )

    fig.update_layout(
        xaxis_tickangle=-45,
        showlegend=False,
        hovermode='x unified'
    )

    # Add value labels
    fig.update_traces(
        texttemplate='%{y}',
        textposition='outside',
        textfont_size=12
    )

    return fig


def main():
    """Main Streamlit app."""
    # Page configuration
    st.set_page_config(
        page_title="RetailPulse Dashboard",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    # Title
    st.title("RetailPulse — Retail Analytics Dashboard")
    st.markdown("---")

    # Check if database exists
    if not check_database_exists():
        return

    # Load summary metrics
    try:
        metrics = load_summary_metrics()
        if metrics['total_sales'] == 0 and metrics['total_customers'] == 0:
            st.warning("No data found in database. Please run the ETL pipeline first.")
    except Exception as e:
        logger.error(f"Error loading metrics: {e}")
        st.error(f"Error connecting to database: {e}")
        st.info("Please run the ETL pipeline first:\n```\npython src/01_setup_db.py\npython src/02_etl_pipeline.py\npython src/03_loyalty_rfm.py\npython src/04_predictive.py\n```")
        return

    # Summary metrics row
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label="Total Sales",
            value=f"₹{metrics['total_sales']:,.0f}",
            delta=None
        )

    with col2:
        st.metric(
            label="Total Customers",
            value=f"{metrics['total_customers']:,}",
            delta=None
        )

    with col3:
        st.metric(
            label="Total Transactions",
            value=f"{metrics['total_transactions']:,}",
            delta=None
        )

    with col4:
        st.metric(
            label="Products Need Restock",
            value=f"{metrics['restock_needed']}",
            delta=None
        )

    st.markdown("---")

    # Loyalty tier summary in sidebar
    st.sidebar.header("Loyalty Tier Summary")
    tier_counts = metrics.get('tier_counts', {})

    col_gold, col_silver, col_bronze = st.sidebar.columns(3)
    with col_gold:
        st.metric("Gold", tier_counts.get('Gold', 0))
    with col_silver:
        st.metric("Silver", tier_counts.get('Silver', 0))
    with col_bronze:
        st.metric("Bronze", tier_counts.get('Bronze', 0))

    st.sidebar.markdown("---")
    st.sidebar.header("Chart Filters")

    # Chart 1: Sales by Store
    st.subheader(" Total Sales by Store")
    sales_df = load_sales_by_store()

    if len(sales_df) > 0:
        # Filter by region
        regions = ['All'] + sorted(sales_df['store_region'].unique().tolist())
        selected_region = st.sidebar.selectbox("Filter by Region (Sales Chart)", regions)

        if selected_region != 'All':
            filtered_sales = sales_df[sales_df['store_region'] == selected_region]
        else:
            filtered_sales = sales_df

        fig1 = chart_sales_by_store(filtered_sales)
        st.plotly_chart(fig1, use_container_width=True)

        with st.expander(" View Sales Data"):
            st.dataframe(filtered_sales, use_container_width=True)
    else:
        st.warning("No sales data available")

    st.markdown("---")

    # Chart 2: Top Products
    st.subheader(" Top 10 Products by Quantity Sold")
    products_df = load_top_products()

    if len(products_df) > 0:
        # Filter by category
        categories = ['All'] + sorted(products_df['product_category'].unique().tolist())
        selected_category = st.sidebar.selectbox("Filter by Category (Products Chart)", categories)

        if selected_category != 'All':
            filtered_products = products_df[products_df['product_category'] == selected_category]
        else:
            filtered_products = products_df

        fig2 = chart_top_products(filtered_products)
        st.plotly_chart(fig2, use_container_width=True)

        with st.expander("View Products Data"):
            display_df = filtered_products[['product_name', 'product_category', 'total_qty', 'total_revenue']]
            display_df.columns = ['Product Name', 'Category', 'Quantity Sold', 'Total Revenue (₹)']
            st.dataframe(display_df, use_container_width=True)
    else:
        st.warning("No products data available")

    st.markdown("---")

    # Chart 3: Loyalty Distribution
    st.subheader("Loyalty Points Distribution by Tier")
    loyalty_df = load_loyalty_distribution()

    if len(loyalty_df) > 0:
        # Tier filter
        tiers = ['All'] + ['Gold', 'Silver', 'Bronze']
        selected_tier = st.sidebar.selectbox("Filter by Tier (Loyalty Chart)", tiers)

        if selected_tier != 'All':
            filtered_loyalty = loyalty_df[loyalty_df['loyalty_status'] == selected_tier]
        else:
            filtered_loyalty = loyalty_df

        fig3 = chart_loyalty_distribution(filtered_loyalty)
        st.plotly_chart(fig3, use_container_width=True)

        # Show tier breakdown
        col1, col2, col3 = st.columns(3)
        with col1:
            gold_count = len(loyalty_df[loyalty_df['loyalty_status'] == 'Gold'])
            gold_avg = loyalty_df[loyalty_df['loyalty_status'] == 'Gold']['total_loyalty_points'].mean()
            st.info(f"**Gold**: {gold_count} customers\n\nAvg Points: {gold_avg:,.0f}" if gold_count > 0 else " **Gold**: 0 customers")
        with col2:
            silver_count = len(loyalty_df[loyalty_df['loyalty_status'] == 'Silver'])
            silver_avg = loyalty_df[loyalty_df['loyalty_status'] == 'Silver']['total_loyalty_points'].mean()
            st.info(f" **Silver**: {silver_count} customers\n\nAvg Points: {silver_avg:,.0f}" if silver_count > 0 else "**Silver**: 0 customers")
        with col3:
            bronze_count = len(loyalty_df[loyalty_df['loyalty_status'] == 'Bronze'])
            bronze_avg = loyalty_df[loyalty_df['loyalty_status'] == 'Bronze']['total_loyalty_points'].mean()
            st.info(f"**Bronze**: {bronze_count} customers\n\nAvg Points: {bronze_avg:,.0f}" if bronze_count > 0 else "**Bronze**: 0 customers")
    else:
        st.warning("No loyalty data available")

    st.markdown("---")

    # Chart 4: At-Risk Customers
    st.subheader("At-Risk Customers by Store (Innovation Bonus)")
    at_risk_df = load_at_risk_customers()

    if len(at_risk_df) > 0:
        fig4 = chart_at_risk_customers(at_risk_df)
        st.plotly_chart(fig4, use_container_width=True)

        total_at_risk = at_risk_df['at_risk_count'].sum()
        st.warning(f"*Total At-Risk Customers**: {total_at_risk} (haven't purchased in >30 days)")

        with st.expander("View At-Risk Data"):
            st.dataframe(at_risk_df, use_container_width=True)
    else:
        st.success("No at-risk customers found!")

    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: gray;'>
        <p>RetailPulse Analytics Dashboard | Built for HCL Hackathon</p>
        <p>Data refreshes on page reload | Use sidebar filters for interactivity</p>
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
