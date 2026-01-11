import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)

# Configuration
START_DATE = datetime(2022, 1, 1)
END_DATE = datetime(2024, 12, 31)
NUM_TRANSACTIONS = 50000

# Master Data Tables
categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books', 'Toys', 'Food & Beverage']

products = {
    'Electronics': ['Laptop', 'Smartphone', 'Tablet', 'Headphones', 'Smart Watch', 'Camera'],
    'Clothing': ['T-Shirt', 'Jeans', 'Jacket', 'Sneakers', 'Dress', 'Sweater'],
    'Home & Garden': ['Coffee Maker', 'Blender', 'Vacuum', 'Plant Pot', 'Lamp', 'Rug'],
    'Sports': ['Yoga Mat', 'Dumbbells', 'Tennis Racket', 'Football', 'Bicycle', 'Running Shoes'],
    'Books': ['Fiction Novel', 'Cookbook', 'Biography', 'Self-Help', 'Children\'s Book'],
    'Toys': ['Board Game', 'Puzzle', 'Action Figure', 'Doll', 'Building Blocks'],
    'Food & Beverage': ['Coffee Beans', 'Chocolate', 'Tea', 'Snacks', 'Wine']
}

regions = ['North', 'South', 'East', 'West', 'Central']
stores = [f'Store_{region}_{i}' for region in regions for i in range(1, 4)]

# Generate Products Table
product_list = []
product_id = 1
for category, items in products.items():
    for item in items:
        base_cost = np.random.uniform(5, 500)
        product_list.append({
            'product_id': product_id,
            'product_name': item,
            'category': category,
            'unit_cost': round(base_cost, 2),
            'unit_price': round(base_cost * np.random.uniform(1.3, 2.5), 2)
        })
        product_id += 1

df_products = pd.DataFrame(product_list)

# Generate Stores Table
store_list = []
for idx, store in enumerate(stores):
    region = store.split('_')[1]
    store_list.append({
        'store_id': idx + 1,
        'store_name': store,
        'region': region,
        'city': f'City_{region}_{random.randint(1, 3)}',
        'state': region,
        'opened_date': START_DATE - timedelta(days=random.randint(365, 1825))
    })

df_stores = pd.DataFrame(store_list)

# Generate Customers Table
customer_list = []
for i in range(1, 5001):
    customer_list.append({
        'customer_id': i,
        'customer_name': f'Customer_{i}',
        'email': f'customer{i}@email.com',
        'join_date': START_DATE + timedelta(days=random.randint(0, 1095)),
        'customer_segment': random.choice(['Regular', 'Premium', 'VIP'])
    })

df_customers = pd.DataFrame(customer_list)

# Generate Sales Transactions
transactions = []
transaction_id = 1

for _ in range(NUM_TRANSACTIONS):
    # Random date with seasonal patterns
    random_days = random.randint(0, (END_DATE - START_DATE).days)
    transaction_date = START_DATE + timedelta(days=random_days)
    
    # Seasonal boost (higher sales in Nov-Dec)
    seasonal_multiplier = 1.5 if transaction_date.month in [11, 12] else 1.0
    
    # Select random product, store, customer
    product = df_products.sample(1).iloc[0]
    store = df_stores.sample(1).iloc[0]
    customer = df_customers.sample(1).iloc[0]
    
    # Quantity with some variance
    if product['category'] == 'Electronics':
        quantity = np.random.choice([1, 2], p=[0.9, 0.1])
    else:
        quantity = np.random.choice([1, 2, 3, 4, 5], p=[0.5, 0.25, 0.15, 0.07, 0.03])
    
    # Calculate amounts
    unit_price = product['unit_price']
    unit_cost = product['unit_cost']
    
    # Apply random discount (0-30%)
    discount_pct = random.choice([0, 0, 0, 0, 5, 10, 15, 20, 30])
    discount_amount = round(unit_price * quantity * (discount_pct / 100), 2)
    
    total_amount = round(unit_price * quantity - discount_amount, 2)
    total_cost = round(unit_cost * quantity, 2)
    profit = round(total_amount - total_cost, 2)
    
    transactions.append({
        'transaction_id': transaction_id,
        'transaction_date': transaction_date,
        'store_id': store['store_id'],
        'customer_id': customer['customer_id'],
        'product_id': product['product_id'],
        'quantity': quantity,
        'unit_price': unit_price,
        'discount_pct': discount_pct,
        'discount_amount': discount_amount,
        'total_amount': total_amount,
        'total_cost': total_cost,
        'profit': profit,
        'payment_method': random.choice(['Credit Card', 'Debit Card', 'Cash', 'Digital Wallet'])
    })
    
    transaction_id += 1

df_transactions = pd.DataFrame(transactions)

# Add calculated columns
df_transactions['year'] = df_transactions['transaction_date'].dt.year
df_transactions['month'] = df_transactions['transaction_date'].dt.month
df_transactions['quarter'] = df_transactions['transaction_date'].dt.quarter
df_transactions['day_of_week'] = df_transactions['transaction_date'].dt.day_name()
df_transactions['profit_margin'] = round((df_transactions['profit'] / df_transactions['total_amount']) * 100, 2)

# Save to CSV files
df_products.to_csv('products.csv', index=False)
df_stores.to_csv('stores.csv', index=False)
df_customers.to_csv('customers.csv', index=False)
df_transactions.to_csv('transactions.csv', index=False)

# Display summary statistics
print("=" * 60)
print("RETAIL SALES DATA GENERATION COMPLETE")
print("=" * 60)
print(f"\nProducts Table: {len(df_products)} records")
print(f"Stores Table: {len(df_stores)} records")
print(f"Customers Table: {len(df_customers)} records")
print(f"Transactions Table: {len(df_transactions)} records")
print(f"\nDate Range: {df_transactions['transaction_date'].min()} to {df_transactions['transaction_date'].max()}")
print(f"Total Revenue: ${df_transactions['total_amount'].sum():,.2f}")
print(f"Total Profit: ${df_transactions['profit'].sum():,.2f}")
print(f"Average Profit Margin: {df_transactions['profit_margin'].mean():.2f}%")
print("\nFiles created:")
print("- products.csv")
print("- stores.csv")
print("- customers.csv")
print("- transactions.csv")