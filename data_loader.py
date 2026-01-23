import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import numpy as np

print("=" * 70)
print("POSTGRESQL DATABASE LOADER - RETAIL SALES ANALYSIS")
print("=" * 70)

DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "retail_analytics",
    "user": "postgres",
    "password": "Success*56"
}

engine = create_engine(
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
    f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()
cur.execute("SELECT version();")
print("Connected to PostgreSQL:", cur.fetchone()[0][:50])
cur.close()
conn.close()

schema_sql = """
DROP TABLE IF EXISTS fact_sales CASCADE;
DROP TABLE IF EXISTS dim_products CASCADE;
DROP TABLE IF EXISTS dim_stores CASCADE;
DROP TABLE IF EXISTS dim_customers CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;

CREATE TABLE dim_products (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    unit_cost NUMERIC(10,2),
    unit_price NUMERIC(10,2),
    total_units_sold INTEGER,
    total_revenue NUMERIC(12,2),
    total_profit NUMERIC(12,2),
    num_sales INTEGER,
    avg_profit_per_sale NUMERIC(10,2),
    margin_pct NUMERIC(5,2),
    margin_category VARCHAR(20),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_stores (
    store_id INTEGER PRIMARY KEY,
    store_name VARCHAR(100),
    region VARCHAR(50),
    city VARCHAR(100),
    state VARCHAR(50),
    opened_date DATE,
    total_revenue NUMERIC(12,2),
    total_profit NUMERIC(12,2),
    num_transactions INTEGER,
    unique_customers INTEGER,
    revenue_per_transaction NUMERIC(10,2),
    revenue_per_customer NUMERIC(10,2),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_customers (
    customer_id INTEGER PRIMARY KEY,
    customer_name VARCHAR(100),
    email VARCHAR(100),
    join_date DATE,
    customer_segment VARCHAR(20),
    lifetime_value NUMERIC(12,2),
    transaction_count INTEGER,
    customer_tenure_days INTEGER,
    avg_order_value NUMERIC(10,2),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE UNIQUE,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    month_name VARCHAR(20),
    week INTEGER,
    day_of_month INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    is_weekend INTEGER,
    is_holiday BOOLEAN DEFAULT FALSE
);

CREATE TABLE fact_sales (
    transaction_id INTEGER PRIMARY KEY,
    transaction_date DATE,
    date_key INTEGER,
    store_id INTEGER REFERENCES dim_stores(store_id),
    customer_id INTEGER REFERENCES dim_customers(customer_id),
    product_id INTEGER REFERENCES dim_products(product_id),
    quantity INTEGER,
    unit_price NUMERIC(10,2),
    discount_pct NUMERIC(5,2),
    discount_amount NUMERIC(10,2),
    total_amount NUMERIC(10,2),
    total_cost NUMERIC(10,2),
    profit NUMERIC(10,2),
    profit_margin_pct NUMERIC(5,2),
    payment_method VARCHAR(50),
    year INTEGER,
    month INTEGER,
    month_name VARCHAR(20),
    quarter INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    week_of_year INTEGER,
    is_weekend INTEGER,
    season VARCHAR(20),
    discount_given INTEGER,
    revenue_per_unit NUMERIC(10,2),
    transaction_size VARCHAR(20)
);
"""

conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()
cur.execute(schema_sql)
conn.commit()
cur.close()
conn.close()

df_products = pd.read_csv("products_cleaned.csv")
df_stores = pd.read_csv("stores_cleaned.csv")
df_customers = pd.read_csv("customers_cleaned.csv")
df_transactions = pd.read_csv("transactions_cleaned.csv")

df_stores["opened_date"] = pd.to_datetime(df_stores["opened_date"])
df_customers["join_date"] = pd.to_datetime(df_customers["join_date"])
df_transactions["transaction_date"] = pd.to_datetime(df_transactions["transaction_date"])

df_products = df_products.replace({np.nan: None})
df_stores = df_stores.replace({np.nan: None})
df_customers = df_customers.replace({np.nan: None})
df_transactions = df_transactions.replace({np.nan: None})

dates = pd.date_range(
    df_transactions["transaction_date"].min(),
    df_transactions["transaction_date"].max()
)

df_date = pd.DataFrame({
    "date_key": dates.strftime("%Y%m%d").astype(int),
    "full_date": dates,
    "year": dates.year,
    "quarter": dates.quarter,
    "month": dates.month,
    "month_name": dates.strftime("%B"),
    "week": dates.isocalendar().week,
    "day_of_month": dates.day,
    "day_of_week": dates.dayofweek,
    "day_name": dates.strftime("%A"),
    "is_weekend": (dates.dayofweek >= 5).astype(int),
    "is_holiday": False
})

df_products.to_sql("dim_products", engine, if_exists="append", index=False, chunksize=100)
df_stores.to_sql("dim_stores", engine, if_exists="append", index=False, chunksize=100)
df_customers.to_sql("dim_customers", engine, if_exists="append", index=False, chunksize=100)
df_date.to_sql("dim_date", engine, if_exists="append", index=False, chunksize=100)

df_transactions["date_key"] = (
    df_transactions["transaction_date"]
    .dt.strftime("%Y%m%d")
    .astype(int)
)

if "profit_margin" in df_transactions.columns:
    df_transactions.drop(columns=["profit_margin"], inplace=True)

fact_sales_columns = [
    "transaction_id", "transaction_date", "date_key",
    "store_id", "customer_id", "product_id",
    "quantity", "unit_price", "discount_pct", "discount_amount",
    "total_amount", "total_cost", "profit", "profit_margin_pct",
    "payment_method", "year", "month", "month_name", "quarter",
    "day_of_week", "day_name", "week_of_year", "is_weekend",
    "season", "discount_given", "revenue_per_unit",
    "transaction_size"
]

df_transactions = df_transactions[fact_sales_columns]

df_transactions.to_sql(
    "fact_sales",
    engine,
    if_exists="append",
    index=False,
    chunksize=100
)

print("=" * 70)
print("DATABASE LOAD COMPLETE")
print("=" * 70)
