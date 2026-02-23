
# Krishni's Contributions - RetailPulse Predictive Analytics

## Overview
Krishni contributed to the **Predictive Analytics Engine** for the RetailPulse retail analytics system, focusing on demand forecasting, inventory management, and customer behavior analysis.

## Files Contributed

### 1. **`04_predictive.py`** - Core Predictive Analytics Engine
Main script implementing three predictive analytics functions:

#### Functions Implemented:
- **`predict_next_month_spend()`** - Forecasts next month's customer spending using 3-month rolling average
- **`predict_restock_flag()`** - Identifies products needing restocking based on 7-day average daily sales vs. current stock
- **`predict_promotion_sensitivity()`** - Classifies customers into HIGH/MEDIUM/LOW promotion response categories

#### Key Features:
- Database connection management with error handling
- SQL-based aggregations for monthly spend and daily sales calculations
- Automatic classification logic with detailed statistics reporting
- Comprehensive logging and exception handling
- Batch database updates for predictions

### 2. **`test_predictive.py`** - Comprehensive Unit Tests
Complete test suite covering all predictive analytics functions:

#### Test Classes:
- `TestPredictNextMonthSpend` - 5 tests for spend forecast calculations
- `TestPredictRestockFlag` - 5 tests for inventory thresholds
- `TestPredictPromotionSensitivity` - 8 tests for customer classification
- `TestPromotionSensitivitySQL` - SQL query validation
- `TestSpendForecastSQL` - Monthly aggregation logic
- `TestRestockFlagSQL` - Stock level queries
- `TestAnalyticsSummaryTable` - Database operations
- `TestEdgeCases` - Boundary and error scenarios

#### Coverage:
- Edge cases (negative stock, large values, decimal precision)
- Boundary conditions (50%, 20% thresholds)
- SQL CASE WHEN logic validation
- Database insert/update operations

## Key Algorithms

### Spend Forecast
```
predicted_spend = average(last_3_months_spend)
```

### Restock Flag
```
IF projected_weekly_demand > current_stock THEN flag = 1
```

### Promotion Sensitivity
```
response_rate = promo_purchases / total_purchases
HIGH: >50% | MEDIUM: 20-50% | LOW: <20%
```

## Technology Stack
- Python 3.x
- SQLite3
- Pandas
- pytest
