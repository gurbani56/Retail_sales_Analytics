import psycopg2
import pandas as pd

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'user': 'postgres',
    'password': 'Success*56',  
    'database': 'retail_sales_analytics'
}

print("LOADING CSV DATA INTO POSTGRESQL")

try:
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    print("✓ Connected to PostgreSQL\n")
except Exception as e:
    print(f"✗ Connection failed: {e}")
    print("\nMake sure:")
    print("1. PostgreSQL is running")
    print("2. Database 'retail_analytics' exists")
    print("3. Your password is correct")
    exit(1)

print("Loading products...")
try:
    df = pd.read_csv('products.csv')
    for _, row in df.iterrows():
        cursor.execute('''
            INSERT INTO dim_products (product_id, product_name, category, unit_cost, unit_price)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (product_id) DO NOTHING
        ''', (int(row['product_id']), row['product_name'], row['category'], 
              float(row['unit_cost']), float(row['unit_price'])))
    conn.commit()
    print(f"✓ Loaded {len(df)} products")
except Exception as e:
    print(f"✗ Error loading products: {e}")
    conn.rollback()


print("Loading stores...")
try:
    df = pd.read_csv('stores.csv')
    for _, row in df.iterrows():
        cursor.execute('''
            INSERT INTO dim_stores (store_id, store_name, region, city, state, opened_date)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (store_id) DO NOTHING
        ''', (int(row['store_id']), row['store_name'], row['region'], 
              row['city'], row['state'], row['opened_date']))
    conn.commit()
    print(f"✓ Loaded {len(df)} stores")
except Exception as e:
    print(f"✗ Error loading stores: {e}")
    conn.rollback()


print("Loading customers...")
try:
    df = pd.read_csv('customers.csv')
    batch_size = 1000
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i+batch_size]
        for _, row in batch.iterrows():
            cursor.execute('''
                INSERT INTO dim_customers (customer_id, customer_name, email, join_date, customer_segment)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (customer_id) DO NOTHING
            ''', (int(row['customer_id']), row['customer_name'], row['email'], 
                  row['join_date'], row['customer_segment']))
        conn.commit()
        print(f"  Progress: {min(i+batch_size, len(df))}/{len(df)} customers", end='\r')
    print(f"\n✓ Loaded {len(df)} customers")
except Exception as e:
    print(f"\n✗ Error loading customers: {e}")
    conn.rollback()


print("Loading transactions...")
try:
    df = pd.read_csv('transactions.csv')
    batch_size = 1000
    total = len(df)
    
    for i in range(0, total, batch_size):
        batch = df.iloc[i:i+batch_size]
        for _, row in batch.iterrows():
            cursor.execute('''
                INSERT INTO fact_sales 
                (transaction_id, transaction_date, store_id, customer_id, product_id,
                 quantity, unit_price, discount_pct, discount_amount, total_amount,
                 total_cost, profit, profit_margin, payment_method)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (transaction_id) DO NOTHING
            ''', (int(row['transaction_id']), row['transaction_date'], 
                  int(row['store_id']), int(row['customer_id']), int(row['product_id']),
                  int(row['quantity']), float(row['unit_price']), float(row['discount_pct']),
                  float(row['discount_amount']), float(row['total_amount']), 
                  float(row['total_cost']), float(row['profit']), 
                  float(row['profit_margin']), row['payment_method']))
        conn.commit()
        print(f"  Progress: {min(i+batch_size, total)}/{total} transactions", end='\r')
    
    print(f"\n✓ Loaded {total} transactions")
except Exception as e:
    print(f"\n✗ Error loading transactions: {e}")
    conn.rollback()


print("\n" + "=" * 70)
print("VERIFICATION")
print("=" * 70)

cursor.execute("SELECT COUNT(*) FROM dim_products")
print(f"Products: {cursor.fetchone()[0]:,} records")

cursor.execute("SELECT COUNT(*) FROM dim_stores")
print(f"Stores: {cursor.fetchone()[0]:,} records")

cursor.execute("SELECT COUNT(*) FROM dim_customers")
print(f"Customers: {cursor.fetchone()[0]:,} records")

cursor.execute("SELECT COUNT(*) FROM fact_sales")
print(f"Transactions: {cursor.fetchone()[0]:,} records")

cursor.execute('''
    SELECT 
        ROUND(SUM(total_amount)::numeric, 2) as revenue,
        ROUND(SUM(profit)::numeric, 2) as profit
    FROM fact_sales
''')
result = cursor.fetchone()
print(f"\nTotal Revenue: ${result[0]:,}")
print(f"Total Profit: ${result[1]:,}")

cursor.close()
conn.close()

print("✓ DATA LOADED SUCCESSFULLY!")

