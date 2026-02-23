
# Vidita's Contributions to RetailPulse

## Overview
Vidita contributed critical visualization and dashboard components to the RetailPulse retail analytics project, enabling stakeholders to interact with and understand complex retail data through intuitive, multi-format dashboards.

## Files Contributed

### 1. **05_dashboard.py** - Static Visualization Dashboard
**Purpose**: Generates a comprehensive 2×2 grid dashboard with static PNG images.

**Key Features**:
- **Chart 1**: Total Sales by Store (vertical bar chart with Rupee formatting)
- **Chart 2**: Top 10 Products by Quantity (horizontal bar chart with category color-coding)
- **Chart 3**: Loyalty Points Distribution (overlapping histograms by tier: Bronze, Silver, Gold)
- **Chart 4 (Bonus)**: At-Risk Customers per Store (innovation feature for identifying retention issues)

**Technical Highlights**:
- Custom Rupee currency formatter (₹ symbol with Crore, Lakh, Thousand scaling)
- Matplotlib-based visualization with professional styling
- Error handling with individual chart exception management
- Generates both combined dashboard and individual chart PNG files
- Outputs to `output/dashboard.png` and `output/chart*.png`

**Dependencies**: sqlite3, pandas, matplotlib

---

### 2. **05_dashboard_streamlit.py** - Interactive Web Dashboard
**Purpose**: Delivers an interactive, real-time dashboard using Streamlit framework with Plotly visualizations.

**Key Features**:
- **Summary Metrics**: Top KPIs (Total Sales, Customer Count, Transactions, Restock Needed)
- **Loyalty Tier Breakdown**: Gold/Silver/Bronze customer counts with avg points
- **Interactive Filters**:
  - Region filter for sales chart
  - Category filter for products chart
  - Loyalty tier filter for distribution
- **Dynamic Data Tables**: Expandable data views for each chart
- **At-Risk Customer Alerts**: Identifies customers with >30 days since last purchase

**Technical Highlights**:
- Streamlit caching decorator (`@st.cache_data`) for performance optimization
- Plotly express for interactive visualizations
- Safe query execution with error handling
- Responsive layout with multi-column metrics
- Sidebar filters and navigation
- Threshold annotations on loyalty points chart

**Dependencies**: streamlit, plotly, pandas, sqlite3

---

### 3. **conftest.py** - Test Fixtures & Configuration
**Purpose**: Provides pytest fixtures for unit testing across the project.

**Key Contributions**:
- `temp_db`: Temporary SQLite database setup/teardown
- `temp_db_with_tables`: Pre-configured database with all schema tables
- `sample_*_df` fixtures: Sample DataFrames for stores, products, customers, transactions, line items, loyalty rules
- `insert_sample_data()`: Helper function for test data population
- Complete schema creation for:
  - Core tables (stores, products, customer_details, transactions, line items)
  - Rejection tracking tables (stores_rejected, products_rejected, etc.)
  - Analytics tables (rfm_summary, customer_predictions, customer_loyalty)

**Test Data Examples**:
- 3 sample stores across regions (Mumbai, Delhi, Chennai)
- 3 products across categories (Electronics, Grocery, Apparel)
- 4 test customers with transaction history
- 5 transactions with line items for comprehensive testing

### 4. **Database Schema & ER Design**
**Purpose**: Defines the relational data model and entity relationships for RetailPulse analytics.

**Core Tables**:
- **stores**: Store ID, name, region, location
- **products**: Product ID, name, category, restock_needed flag
- **customer_details**: Customer ID, loyalty tier, segment, points balance
- **transactions** (store_sales_header): Transaction ID, store ID, customer ID, date, total amount
- **line_items** (store_sales_line_items): Line item ID, transaction ID, product ID, quantity, amount

**Analytics Tables**:
- **rfm_summary**: RFM scoring for customer segmentation
- **customer_predictions**: Churn risk and loyalty predictions
- **customer_loyalty**: Tier-based loyalty point allocations and thresholds

**ER Relationships**:
- One Store → Many Transactions
- One Customer → Many Transactions
- One Transaction → Many Line Items
- One Product → Many Line Items
- Customer → Loyalty Tier Rules (Bronze, Silver, Gold)

**Key Constraints**:
- Primary keys on all entity tables
- Foreign key relationships for referential integrity
- Rejection tracking tables mirror core schema for data quality monitoring
- Indexes on frequently queried columns (store_id, customer_id, transaction_date)

---

## Usage Examples

### Static Dashboard
```bash
python src/05_dashboard.py
```
Output: High-resolution PNG files suitable for reports and presentations

### Interactive Dashboard
```bash
streamlit run src/05_dashboard_streamlit.py
```
Output: Web-based dashboard with real-time filtering and exploration

### Running Tests
```bash
pytest tests/ -v
```
Uses fixtures from conftest.py for isolated test environments

---

## Key Innovations
✨ **Bonus Chart**: At-Risk customer detection provides actionable insights for customer retention
✨ **Multi-Format Output**: Static (PNG) + Interactive (Streamlit) for different stakeholder needs
✨ **Professional Formatting**: Localized currency symbols, optimized color schemes, responsive layouts
✨ **Robust Error Handling**: Graceful degradation when data unavailable; individual chart failures don't break dashboard
✨ **Performance Optimization**: Streamlit caching reduces database queries; smart fixture design for testing
