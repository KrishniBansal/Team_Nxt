# Team_Nxt - Retail Sales Analytics Platform

A comprehensive data analytics solution for retail sales management, featuring data ingestion, validation, customer loyalty analytics, RFM segmentation, and predictive insights.

---

## 📋 Table of Contents

- [Overview](#overview)
- [Solution Architecture](#solution-architecture)
- [Features](#features)
- [Data Sources](#data-sources)
- [Tech Stack](#tech-stack)
- [Getting Started](#getting-started)

---

## Overview

This project provides an end-to-end retail analytics pipeline that:
- Ingests sales data from multiple CSV/Excel sources
- Validates and cleanses data with comprehensive quality checks
- Stores clean data in a relational database
- Computes customer loyalty points and RFM segmentation
- Generates predictive analytics for business insights
- Visualizes KPIs through interactive dashboards

---

## Solution Architecture

```
                    ┌──────────────────────────┐
                    │        Data Sources       │
                    │  CSV Files (Sales Data)  │
                    │  - stores.csv            │
                    │  - products.csv          │
                    │  - customers.csv         │
                    │  - sales_header.csv      │
                    │  - sales_line_items.csv  │
                    │  - promotions.csv        │
                    │  - loyalty_rules.csv     │
                    └──────────────┬───────────┘
                                   │
                                   ▼
                    ┌──────────────────────────┐
                    │  Data Ingestion Layer    │
                    │  (Python – Pandas)       │
                    │                          │
                    │  ✔ Read CSVs             │
                    │  ✔ Schema Mapping        │
                    └──────────────┬───────────┘
                                   │
                                   ▼
                    ┌──────────────────────────┐
                    │ Data Validation Layer    │
                    │ (Quality Control Engine) │
                    │                          │
                    │  ✔ Null Checks           │
                    │  ✔ Negative Checks       │
                    │  ✔ Datatype Validation   │
                    │  ✔ Special Char Removal  │
                    │  ✔ FK Validation         │
                    │  ✔ Reject Tables         │
                    └──────────────┬───────────┘
                                   │
                        Clean Data │   Rejected Data
                                   │
                                   ▼
                    ┌──────────────────────────┐
                    │   Relational Database    │
                    │     (SQLite / MySQL)     │
                    │                          │
                    │  Core Tables:            │
                    │  - stores                │
                    │  - products              │
                    │  - customer_details      │
                    │  - store_sales_header    │
                    │  - store_sales_line_items│
                    │  - promotion_details     │
                    │  - loyalty_rules         │
                    │                          │
                    │  + reject_tables         │
                    └──────────────┬───────────┘
                                   │
                                   ▼
                    ┌──────────────────────────┐
                    │     Analytics Layer      │
                    │     (Business Logic)     │
                    │                          │
                    │  🔹 Loyalty Engine       │
                    │     - Points per spend   │
                    │     - Bonus logic        │
                    │                          │
                    │  🔹 RFM Segmentation     │
                    │     - Recency            │
                    │     - Frequency          │
                    │     - Monetary           │
                    │     - HS / AR segments   │
                    │                          │
                    │  🔹 Predictive Logic     │
                    │     - Next month spend   │
                    │     - Stock risk         │
                    │     - Promo response     │
                    └──────────────┬───────────┘
                                   │
                                   ▼
                    ┌──────────────────────────┐
                    │     Dashboard Layer      │
                    │     (Matplotlib)         │
                    │                          │
                    │  📊 Total Sales by Store │
                    │  📊 Top 10 Products      │
                    │  📊 Loyalty Distribution │
                    └──────────────────────────┘
```

---

## Features

### 🔄 Data Ingestion
- Automated CSV/Excel file reading
- Flexible schema mapping for diverse data formats

### ✅ Data Validation
- Null value detection and handling
- Negative value checks for numeric fields
- Datatype validation and conversion
- Special character removal
- Foreign key integrity validation
- Rejected records stored in separate tables for audit

### 📊 Analytics Engine
- **Loyalty Engine**: Points calculation per spend with bonus logic
- **RFM Segmentation**: Customer segmentation based on Recency, Frequency, and Monetary value
- **Predictive Analytics**: Forecasting for next month spend, stock risk, and promotion response

### 📈 Dashboard & Visualization
- Total sales by store
- Top 10 performing products
- Customer loyalty distribution charts

---

## Data Sources

| File | Description |
|------|-------------|
| `stores.xlsx` | Store information and locations |
| `Product - 1.xlsx` | Product catalog details |
| `Cutomer Detail- 1.xlsx` | Customer demographics and info |
| `Store sales.xlsx` | Sales transaction headers |
| `Store sales lines 1.xlsx` | Sales line item details |
| `Promotion 1.xlsx` | Promotion campaigns data |
| `Loyalty Rule 1.xlsx` | Loyalty program rules |

---

## Tech Stack

- **Language**: Python
- **Data Processing**: Pandas
- **Database**: SQLite / MySQL
- **Visualization**: Matplotlib
- **Version Control**: Git

---

## Getting Started

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd Team_Nxt
   ```

2. **Install dependencies**
   ```bash
   pip install pandas matplotlib sqlite3
   ```

3. **Run the data pipeline**
   ```bash
   python main.py
   ```

---

## Team

**Team Nxt** - Hackathon Project 2026

---

## License

This project is developed for hackathon demonstration purposes.
