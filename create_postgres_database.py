import psycopg2
from psycopg2 import sql
import pandas as pd

# =====================================================
# DATABASE CONNECTION CONFIGURATION
# =====================================================
# MODIFY THESE VALUES ACCORDING TO YOUR POSTGRESQL SETUP

DB_CONFIG = {
    'host': 'localhost',        # Usually localhost
    'port': 5432,               # Default PostgreSQL port
    'user': 'postgres',         # Your PostgreSQL username
    'password': 'Success*56' # YOUR PASSWORD HERE - CHANGE THIS!
}

DATABASE_NAME = 'retail_analytics'

# =====================================================
# STEP 1: CREATE DATABASE
# =====================================================

print("=" * 70)
print("POSTGRESQL DATABASE SETUP - RETAIL ANALYTICS")
print("=" * 70)

try:
    # Connect to PostgreSQL server (to default 'postgres' database)
    print("\n1. Connecting to PostgreSQL server...")
    conn = psycopg2.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        database='postgres'  # Connect to default database first
    )
    conn.autocommit = True
    cursor = conn.cursor()
    
    # Check if database exists
    cursor.execute(
        "SELECT 1 FROM pg_database WHERE datname = %s",
        (DATABASE_NAME,)
    )
    exists = cursor.fetchone()
    
    if exists:
        print(f"   Database '{DATABASE_NAME}' already exists. Dropping and recreating...")
        # Terminate existing connections
        cursor.execute(f"""
            SELECT pg_terminate_backend(pg_stat_activity.pid)
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = '{DATABASE_NAME}'
            AND pid <> pg_backend_pid()
        """)
        cursor.execute(f"DROP DATABASE {DATABASE_NAME}")
    
    # Create new database
    cursor.execute(f"CREATE DATABASE {DATABASE_NAME}")
    print(f"✓ Database '{DATABASE_NAME}' created successfully")
    
    cursor.close()
    conn.close()
    
except psycopg2.Error as e:
    print(f"✗ Error connecting to PostgreSQL: {e}")
    print("\nPlease check:")
    print("1. PostgreSQL is running")
    print("2. Your username and password are correct")
    print("3. Host and port are correct")
    exit(1)

# =====================================================
# STEP 2: CONNECT TO NEW DATABASE AND CREATE TABLES
# =====================================================

print("\n2. Connecting to retail_analytics database...")
conn = psycopg2.connect(
    host=DB_CONFIG['host'],
    port=DB_CONFIG['port'],
    user=DB_CONFIG['user'],
    password=DB_CONFIG['password'],
    database=DATABASE_NAME
)
cursor = conn.cursor()
print("✓ Connected successfully")

# =====================================================
# CREATE DIMENSION TABLES
# =====================================================

print("\n3. Creating dimension tables...")

# Products Dimension
cursor.execute('''
CREATE TABLE dim_products (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL,
    unit_cost DECIMAL(10, 2) NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
''')
print("   ✓ dim_products table created")

# Stores Dimension
cursor.execute('''
CREATE TABLE dim_stores (
    store_id INTEGER PRIMARY KEY,
    store_name VARCHAR(100) NOT NULL,
    region VARCHAR(50) NOT NULL,
    city VARCHAR(100),
    state VARCHAR(50),
    opened_date DATE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
''')
print("   ✓ dim_stores table created")

# Customers Dimension
cursor.execute('''
CREATE TABLE dim_customers (
    customer_id INTEGER PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    email VARCHAR(100),
    join_date DATE,
    customer_segment VARCHAR(20),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
''')
print("   ✓ dim_customers table created")

# Date Dimension (for time intelligence)
cursor.execute('''
CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    month_name VARCHAR(20),
    week INTEGER,
    day_of_month INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    is_weekend BOOLEAN,
    is_holiday BOOLEAN
)
''')
print("   ✓ dim_date table created")

# =====================================================
# CREATE FACT TABLE
# =====================================================

print("\n4. Creating fact table...")

cursor.execute('''
CREATE TABLE fact_sales (
    transaction_id INTEGER PRIMARY KEY,
    transaction_date DATE NOT NULL,
    date_key INTEGER,
    store_id INTEGER,
    customer_id INTEGER,
    product_id INTEGER,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    discount_pct DECIMAL(5, 2),
    discount_amount DECIMAL(10, 2),
    total_amount DECIMAL(10, 2) NOT NULL,
    total_cost DECIMAL(10, 2) NOT NULL,
    profit DECIMAL(10, 2) NOT NULL,
    profit_margin DECIMAL(5, 2),
    payment_method VARCHAR(50),
    FOREIGN KEY (store_id) REFERENCES dim_stores(store_id),
    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES dim_products(product_id),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
)
''')
print("   ✓ fact_sales table created")

conn.commit()

# =====================================================
# LOAD DATA FROM CSV FILES
# =====================================================

print("\n5. Loading data from CSV files...")

try:
    # Load Products
    df_products = pd.read_csv('products.csv')
    for _, row in df_products.iterrows():
        cursor.execute('''
            INSERT INTO dim_products (product_id, product_name, category, unit_cost, unit_price)
            VALUES (%s, %s, %s, %s, %s)
        ''', (row['product_id'], row['product_name'], row['category'], 
              row['unit_cost'], row['unit_price']))
    print(f"   ✓ Loaded {len(df_products)} products")
    
    # Load Stores
    df_stores = pd.read_csv('stores.csv')
    for _, row in df_stores.iterrows():
        cursor.execute('''
            INSERT INTO dim_stores (store_id, store_name, region, city, state, opened_date)
            VALUES (%s, %s, %s, %s, %s, %s)
        ''', (row['store_id'], row['store_name'], row['region'], 
              row['city'], row['state'], row['opened_date']))
    print(f"   ✓ Loaded {len(df_stores)} stores")
    
    # Load Customers
    df_customers = pd.read_csv('customers.csv')
    for _, row in df_customers.iterrows():
        cursor.execute('''
            INSERT INTO dim_customers (customer_id, customer_name, email, join_date, customer_segment)
            VALUES (%s, %s, %s, %s, %s)
        ''', (row['customer_id'], row['customer_name'], row['email'], 
              row['join_date'], row['customer_segment']))
    print(f"   ✓ Loaded {len(df_customers)} customers")
    
    # Load Transactions
    df_transactions = pd.read_csv('transactions.csv')
    batch_size = 1000
    total_rows = len(df_transactions)
    
    for i in range(0, total_rows, batch_size):
        batch = df_transactions.iloc[i:i+batch_size]
        for _, row in batch.iterrows():
            cursor.execute('''
                INSERT INTO fact_sales 
                (transaction_id, transaction_date, store_id, customer_id, product_id,
                 quantity, unit_price, discount_pct, discount_amount, total_amount,
                 total_cost, profit, profit_margin, payment_method)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (row['transaction_id'], row['transaction_date'], row['store_id'],
                  row['customer_id'], row['product_id'], row['quantity'], row['unit_price'],
                  row['discount_pct'], row['discount_amount'], row['total_amount'],
                  row['total_cost'], row['profit'], row['profit_margin'], row['payment_method']))
        
        conn.commit()
        print(f"   ✓ Loaded {min(i+batch_size, total_rows)}/{total_rows} transactions", end='\r')
    
    print(f"\n   ✓ Loaded {len(df_transactions)} transactions successfully")
    
except FileNotFoundError as e:
    print(f"\n✗ Error: CSV file not found - {e}")
    print("Make sure your CSV files are in the same directory as this script")
    conn.close()
    exit(1)

conn.commit()

# =====================================================
# CREATE INDEXES FOR PERFORMANCE
# =====================================================

print("\n6. Creating indexes for query optimization...")

cursor.execute('CREATE INDEX idx_sales_date ON fact_sales(transaction_date)')
cursor.execute('CREATE INDEX idx_sales_store ON fact_sales(store_id)')
cursor.execute('CREATE INDEX idx_sales_product ON fact_sales(product_id)')
cursor.execute('CREATE INDEX idx_sales_customer ON fact_sales(customer_id)')
cursor.execute('CREATE INDEX idx_products_category ON dim_products(category)')
cursor.execute('CREATE INDEX idx_stores_region ON dim_stores(region)')

conn.commit()
print("   ✓ All indexes created")

# =====================================================
# VERIFY DATA LOAD
# =====================================================

print("\n" + "=" * 70)
print("DATABASE VERIFICATION")
print("=" * 70)

cursor.execute("SELECT COUNT(*) FROM dim_products")
print(f"\nProducts: {cursor.fetchone()[0]:,} records")

cursor.execute("SELECT COUNT(*) FROM dim_stores")
print(f"Stores: {cursor.fetchone()[0]:,} records")

cursor.execute("SELECT COUNT(*) FROM dim_customers")
print(f"Customers: {cursor.fetchone()[0]:,} records")

cursor.execute("SELECT COUNT(*) FROM fact_sales")
print(f"Transactions: {cursor.fetchone()[0]:,} records")

# Run sample query
cursor.execute('''
    SELECT 
        SUM(total_amount) as total_revenue,
        SUM(profit) as total_profit,
        ROUND(AVG(profit_margin)::numeric, 2) as avg_margin
    FROM fact_sales
''')

result = cursor.fetchone()
print(f"\nTotal Revenue: ${result[0]:,.2f}")
print(f"Total Profit: ${result[1]:,.2f}")
print(f"Average Profit Margin: {result[2]}%")

# Close connection
cursor.close()
conn.close()

print("\n" + "=" * 70)
print("✓ DATABASE SETUP COMPLETE!")
print("=" * 70)
print(f"\nDatabase Name: {DATABASE_NAME}")
print(f"Host: {DB_CONFIG['host']}")
print(f"Port: {DB_CONFIG['port']}")
