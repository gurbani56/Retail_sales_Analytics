import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')


df_products = pd.read_csv('products.csv')
df_stores = pd.read_csv('stores.csv')
df_customers = pd.read_csv('customers.csv')
df_transactions = pd.read_csv('transactions.csv')


def assess_data_quality(df, name):
    print(f"\n{name} Table:")
    print(f"  Shape: {df.shape}")
    print(f"  Missing Values:")
    missing = df.isnull().sum()
    if missing.sum() > 0:
        print(missing[missing > 0])
    else:
        print("    None")
    
    print(f"  Duplicates: {df.duplicated().sum()}")
    print(f"  Memory Usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

assess_data_quality(df_products, "PRODUCTS")
assess_data_quality(df_stores, "STORES")
assess_data_quality(df_customers, "CUSTOMERS")
assess_data_quality(df_transactions, "TRANSACTIONS")

# data cleaning
# check for nulls in critical columns
critical_cols_transactions = ['transaction_id', 'transaction_date', 'product_id', 
                               'store_id', 'customer_id', 'total_amount']

for col in critical_cols_transactions:
    null_count = df_transactions[col].isnull().sum()
    if null_count > 0:
        print(f" {col}: {null_count} missing values")
    else:
        print(f"  {col}: No missing values")

# filling missing discount values with 0
df_transactions['discount_pct'] = df_transactions['discount_pct'].fillna(0)
df_transactions['discount_amount'] = df_transactions['discount_amount'].fillna(0)

# removing duplicates
initial_count = len(df_transactions)
df_transactions = df_transactions.drop_duplicates(subset=['transaction_id'])
removed = initial_count - len(df_transactions)
print(f"  Removed {removed} duplicate transactions")

# data type conversion
df_transactions['transaction_date'] = pd.to_datetime(df_transactions['transaction_date'])
df_stores['opened_date'] = pd.to_datetime(df_stores['opened_date'])
df_customers['join_date'] = pd.to_datetime(df_customers['join_date'])


def detect_outliers(df, column):
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    outliers = df[(df[column] < lower_bound) | (df[column] > upper_bound)]
    return outliers, lower_bound, upper_bound

outliers, lower, upper = detect_outliers(df_transactions, 'total_amount')
print(f"  Total Amount Outliers: {len(outliers)} transactions")
print(f"  Bounds: [{lower:.2f}, {upper:.2f}]")

# Validating Business Logic
# Check for negative values
negative_checks = {
    'quantity': (df_transactions['quantity'] < 0).sum(),
    'total_amount': (df_transactions['total_amount'] < 0).sum(),
    'profit': (df_transactions['profit'] < 0).sum()
}

for field, count in negative_checks.items():
    if count > 0:
        print(f" {field}: {count} negative values")
    else:
        print(f" {field}: No negative values")

# Validating profit calculation
df_transactions['calculated_profit'] = df_transactions['total_amount'] - df_transactions['total_cost']
profit_mismatch = abs(df_transactions['profit'] - df_transactions['calculated_profit']) > 0.01
print(f"  Profit calculation mismatches: {profit_mismatch.sum()}")

# data transformation
# creating time-based features

df_transactions['year'] = df_transactions['transaction_date'].dt.year
df_transactions['month'] = df_transactions['transaction_date'].dt.month
df_transactions['month_name'] = df_transactions['transaction_date'].dt.month_name()
df_transactions['quarter'] = df_transactions['transaction_date'].dt.quarter
df_transactions['day_of_week'] = df_transactions['transaction_date'].dt.dayofweek
df_transactions['day_name'] = df_transactions['transaction_date'].dt.day_name()
df_transactions['week_of_year'] = df_transactions['transaction_date'].dt.isocalendar().week
df_transactions['is_weekend'] = df_transactions['day_of_week'].isin([5, 6]).astype(int)

# Creating season
def get_season(month):
    if month in [12, 1, 2]:
        return 'Winter'
    elif month in [3, 4, 5]:
        return 'Spring'
    elif month in [6, 7, 8]:
        return 'Summer'
    else:
        return 'Fall'

df_transactions['season'] = df_transactions['month'].apply(get_season)


# Profit margin percentage
df_transactions['profit_margin_pct'] = (
    df_transactions['profit'] / df_transactions['total_amount'] * 100
).round(2)

# Discount effectiveness
df_transactions['discount_given'] = (df_transactions['discount_pct'] > 0).astype(int)

# Revenue per unit
df_transactions['revenue_per_unit'] = (
    df_transactions['total_amount'] / df_transactions['quantity']
).round(2)

# Transaction size category
def categorize_transaction(amount):
    if amount < 50:
        return 'Small'
    elif amount < 200:
        return 'Medium'
    elif amount < 500:
        return 'Large'
    else:
        return 'Very Large'

df_transactions['transaction_size'] = df_transactions['total_amount'].apply(categorize_transaction)

# Customer features
# Customer lifetime value
customer_ltv = df_transactions.groupby('customer_id').agg({
    'total_amount': 'sum',
    'transaction_id': 'count',
    'transaction_date': ['min', 'max']
}).reset_index()

customer_ltv.columns = ['customer_id', 'lifetime_value', 'transaction_count', 'first_purchase', 'last_purchase']
customer_ltv['customer_tenure_days'] = (customer_ltv['last_purchase'] - customer_ltv['first_purchase']).dt.days
customer_ltv['avg_order_value'] = customer_ltv['lifetime_value'] / customer_ltv['transaction_count']

# Merge back to customers
df_customers = df_customers.merge(customer_ltv[['customer_id', 'lifetime_value', 'transaction_count', 
                                                  'customer_tenure_days', 'avg_order_value']], 
                                   on='customer_id', how='left')

print(" Created: lifetime_value, transaction_count, customer_tenure_days, avg_order_value")

# Creating Product features
# Product performance metrics
product_metrics = df_transactions.groupby('product_id').agg({
    'quantity': 'sum',
    'total_amount': 'sum',
    'profit': 'sum',
    'transaction_id': 'count'
}).reset_index()

product_metrics.columns = ['product_id', 'total_units_sold', 'total_revenue', 'total_profit', 'num_sales']
product_metrics['avg_profit_per_sale'] = product_metrics['total_profit'] / product_metrics['num_sales']

df_products = df_products.merge(product_metrics, on='product_id', how='left')

# Product margin category
df_products['margin_pct'] = ((df_products['unit_price'] - df_products['unit_cost']) / 
                              df_products['unit_price'] * 100).round(2)

def categorize_margin(margin):
    if margin < 30:
        return 'Low Margin'
    elif margin < 50:
        return 'Medium Margin'
    else:
        return 'High Margin'

df_products['margin_category'] = df_products['margin_pct'].apply(categorize_margin)

print("  ✓ Created: total_units_sold, total_revenue, margin_category")

# Creating store features
# Store performance metrics
store_metrics = df_transactions.groupby('store_id').agg({
    'total_amount': 'sum',
    'profit': 'sum',
    'transaction_id': 'count',
    'customer_id': 'nunique'
}).reset_index()

store_metrics.columns = ['store_id', 'total_revenue', 'total_profit', 'num_transactions', 'unique_customers']
store_metrics['revenue_per_transaction'] = store_metrics['total_revenue'] / store_metrics['num_transactions']
store_metrics['revenue_per_customer'] = store_metrics['total_revenue'] / store_metrics['unique_customers']

df_stores = df_stores.merge(store_metrics, on='store_id', how='left')

print(" Created: total_revenue, revenue_per_transaction, unique_customers")

# Create Master Analytical Dataset
print("\n Creating Master Analytical Dataset...")

df_master = df_transactions.merge(df_products[['product_id', 'product_name', 'category', 'margin_category']], 
                                   on='product_id', how='left')
df_master = df_master.merge(df_stores[['store_id', 'store_name', 'region', 'city']], 
                             on='store_id', how='left')
df_master = df_master.merge(df_customers[['customer_id', 'customer_segment', 'lifetime_value']], 
                             on='customer_id', how='left')


# Data Analytics
# Descriptive Statistics
print("\n Transaction Summary Statistics:")
summary_stats = df_transactions[['quantity', 'total_amount', 'profit', 'profit_margin_pct']].describe()
print(summary_stats)

# Revenue Analysis
print("\n Revenue Breakdown:")
print(f"  Total Revenue: ${df_transactions['total_amount'].sum():,.2f}")
print(f"  Total Profit: ${df_transactions['profit'].sum():,.2f}")
print(f"  Average Transaction Value: ${df_transactions['total_amount'].mean():.2f}")
print(f"  Median Transaction Value: ${df_transactions['total_amount'].median():.2f}")
print(f"  Overall Profit Margin: {(df_transactions['profit'].sum() / df_transactions['total_amount'].sum() * 100):.2f}%")

# Time-based Analysis
print("\n Temporal Patterns:")
yearly_sales = df_transactions.groupby('year')['total_amount'].sum()
print("\nRevenue by Year:")
print(yearly_sales)

monthly_avg = df_transactions.groupby('month_name')['total_amount'].mean().sort_values(ascending=False)
print("\nTop 3 Months by Avg Transaction Value:")
print(monthly_avg.head(3))

# Category Performance
print("\n Category Performance:")
category_perf = df_master.groupby('category').agg({
    'total_amount': 'sum',
    'profit': 'sum',
    'transaction_id': 'count'
}).round(2)
category_perf.columns = ['Revenue', 'Profit', 'Transactions']
category_perf = category_perf.sort_values('Revenue', ascending=False)
print(category_perf)

# Regional Performance
print("\n Regional Performance:")
regional_perf = df_master.groupby('region').agg({
    'total_amount': 'sum',
    'profit': 'sum',
    'transaction_id': 'count'
}).round(2)
regional_perf.columns = ['Revenue', 'Profit', 'Transactions']
print(regional_perf)

# Customer Segment Analysis
print("\n Customer Segment Analysis:")
segment_analysis = df_master.groupby('customer_segment').agg({
    'customer_id': 'nunique',
    'total_amount': ['sum', 'mean'],
    'profit': 'sum'
}).round(2)
print(segment_analysis)


# RFM Analysis (Recency, Frequency, Monetary)
print("\n RFM Analysis...")

analysis_date = df_transactions['transaction_date'].max() + pd.Timedelta(days=1)

rfm = df_transactions.groupby('customer_id').agg({
    'transaction_date': lambda x: (analysis_date - x.max()).days,  # Recency
    'transaction_id': 'count',  # Frequency
    'total_amount': 'sum'  # Monetary
}).reset_index()

rfm.columns = ['customer_id', 'recency', 'frequency', 'monetary']

# Create RFM scores
rfm['r_score'] = pd.qcut(rfm['recency'], 4, labels=[4, 3, 2, 1])
rfm['f_score'] = pd.qcut(rfm['frequency'].rank(method='first'), 4, labels=[1, 2, 3, 4])
rfm['m_score'] = pd.qcut(rfm['monetary'], 4, labels=[1, 2, 3, 4])

rfm['rfm_score'] = rfm['r_score'].astype(str) + rfm['f_score'].astype(str) + rfm['m_score'].astype(str)

print("  RFM Segments Distribution:")
print(rfm['rfm_score'].value_counts().head(10))

# Cohort Analysis
print("\n Cohort Analysis...")

df_transactions['order_month'] = df_transactions['transaction_date'].dt.to_period('M')
df_transactions['cohort'] = df_transactions.groupby('customer_id')['transaction_date'].transform('min').dt.to_period('M')

cohort_data = df_transactions.groupby(['cohort', 'order_month']).agg({
    'customer_id': 'nunique'
}).reset_index()

cohort_data['period_number'] = (cohort_data['order_month'] - cohort_data['cohort']).apply(lambda x: x.n)

cohort_pivot = cohort_data.pivot_table(index='cohort', columns='period_number', values='customer_id')

print("  Cohort retention table created")
print(f"  Cohorts tracked: {len(cohort_pivot)}")

# Product Affinity Analysis
print("\n Product Basket Analysis...")

# Find products frequently bought together
basket = df_transactions.groupby(['customer_id', 'transaction_date'])['product_id'].apply(list).reset_index()
basket['basket_size'] = basket['product_id'].apply(len)

print(f"  Average basket size: {basket['basket_size'].mean():.2f} items")
print(f"  Transactions with multiple items: {(basket['basket_size'] > 1).sum()}")




import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')


df_products = pd.read_csv('products.csv')
df_stores = pd.read_csv('stores.csv')
df_customers = pd.read_csv('customers.csv')
df_transactions = pd.read_csv('transactions.csv')


def assess_data_quality(df, name):
    print(f"\n{name} Table:")
    print(f"  Shape: {df.shape}")
    print(f"  Missing Values:")
    missing = df.isnull().sum()
    if missing.sum() > 0:
        print(missing[missing > 0])
    else:
        print("    None")
    
    print(f"  Duplicates: {df.duplicated().sum()}")
    print(f"  Memory Usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

assess_data_quality(df_products, "PRODUCTS")
assess_data_quality(df_stores, "STORES")
assess_data_quality(df_customers, "CUSTOMERS")
assess_data_quality(df_transactions, "TRANSACTIONS")

# data cleaning
# check for nulls in critical columns
critical_cols_transactions = ['transaction_id', 'transaction_date', 'product_id', 
                               'store_id', 'customer_id', 'total_amount']

for col in critical_cols_transactions:
    null_count = df_transactions[col].isnull().sum()
    if null_count > 0:
        print(f" {col}: {null_count} missing values")
    else:
        print(f"  {col}: No missing values")

# filling missing discount values with 0
df_transactions['discount_pct'] = df_transactions['discount_pct'].fillna(0)
df_transactions['discount_amount'] = df_transactions['discount_amount'].fillna(0)

# removing duplicates
initial_count = len(df_transactions)
df_transactions = df_transactions.drop_duplicates(subset=['transaction_id'])
removed = initial_count - len(df_transactions)
print(f"  Removed {removed} duplicate transactions")

# data type conversion
df_transactions['transaction_date'] = pd.to_datetime(df_transactions['transaction_date'])
df_stores['opened_date'] = pd.to_datetime(df_stores['opened_date'])
df_customers['join_date'] = pd.to_datetime(df_customers['join_date'])


def detect_outliers(df, column):
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    outliers = df[(df[column] < lower_bound) | (df[column] > upper_bound)]
    return outliers, lower_bound, upper_bound

outliers, lower, upper = detect_outliers(df_transactions, 'total_amount')
print(f"  Total Amount Outliers: {len(outliers)} transactions")
print(f"  Bounds: [{lower:.2f}, {upper:.2f}]")

# Validating Business Logic
# Check for negative values
negative_checks = {
    'quantity': (df_transactions['quantity'] < 0).sum(),
    'total_amount': (df_transactions['total_amount'] < 0).sum(),
    'profit': (df_transactions['profit'] < 0).sum()
}

for field, count in negative_checks.items():
    if count > 0:
        print(f" {field}: {count} negative values")
    else:
        print(f" {field}: No negative values")

# Validating profit calculation
df_transactions['calculated_profit'] = df_transactions['total_amount'] - df_transactions['total_cost']
profit_mismatch = abs(df_transactions['profit'] - df_transactions['calculated_profit']) > 0.01
print(f"  Profit calculation mismatches: {profit_mismatch.sum()}")

# data transformation
# creating time-based features

df_transactions['year'] = df_transactions['transaction_date'].dt.year
df_transactions['month'] = df_transactions['transaction_date'].dt.month
df_transactions['month_name'] = df_transactions['transaction_date'].dt.month_name()
df_transactions['quarter'] = df_transactions['transaction_date'].dt.quarter
df_transactions['day_of_week'] = df_transactions['transaction_date'].dt.dayofweek
df_transactions['day_name'] = df_transactions['transaction_date'].dt.day_name()
df_transactions['week_of_year'] = df_transactions['transaction_date'].dt.isocalendar().week
df_transactions['is_weekend'] = df_transactions['day_of_week'].isin([5, 6]).astype(int)

# Creating season
def get_season(month):
    if month in [12, 1, 2]:
        return 'Winter'
    elif month in [3, 4, 5]:
        return 'Spring'
    elif month in [6, 7, 8]:
        return 'Summer'
    else:
        return 'Fall'

df_transactions['season'] = df_transactions['month'].apply(get_season)


# Profit margin percentage
df_transactions['profit_margin_pct'] = (
    df_transactions['profit'] / df_transactions['total_amount'] * 100
).round(2)

# Discount effectiveness
df_transactions['discount_given'] = (df_transactions['discount_pct'] > 0).astype(int)

# Revenue per unit
df_transactions['revenue_per_unit'] = (
    df_transactions['total_amount'] / df_transactions['quantity']
).round(2)

# Transaction size category
def categorize_transaction(amount):
    if amount < 50:
        return 'Small'
    elif amount < 200:
        return 'Medium'
    elif amount < 500:
        return 'Large'
    else:
        return 'Very Large'

df_transactions['transaction_size'] = df_transactions['total_amount'].apply(categorize_transaction)

# Customer features
# Customer lifetime value
customer_ltv = df_transactions.groupby('customer_id').agg({
    'total_amount': 'sum',
    'transaction_id': 'count',
    'transaction_date': ['min', 'max']
}).reset_index()

customer_ltv.columns = ['customer_id', 'lifetime_value', 'transaction_count', 'first_purchase', 'last_purchase']
customer_ltv['customer_tenure_days'] = (customer_ltv['last_purchase'] - customer_ltv['first_purchase']).dt.days
customer_ltv['avg_order_value'] = customer_ltv['lifetime_value'] / customer_ltv['transaction_count']

# Merge back to customers
df_customers = df_customers.merge(customer_ltv[['customer_id', 'lifetime_value', 'transaction_count', 
                                                  'customer_tenure_days', 'avg_order_value']], 
                                   on='customer_id', how='left')

print("  ✓ Created: lifetime_value, transaction_count, customer_tenure_days, avg_order_value")

# Creating Product features
# Product performance metrics
product_metrics = df_transactions.groupby('product_id').agg({
    'quantity': 'sum',
    'total_amount': 'sum',
    'profit': 'sum',
    'transaction_id': 'count'
}).reset_index()

product_metrics.columns = ['product_id', 'total_units_sold', 'total_revenue', 'total_profit', 'num_sales']
product_metrics['avg_profit_per_sale'] = product_metrics['total_profit'] / product_metrics['num_sales']

df_products = df_products.merge(product_metrics, on='product_id', how='left')

# Product margin category
df_products['margin_pct'] = ((df_products['unit_price'] - df_products['unit_cost']) / 
                              df_products['unit_price'] * 100).round(2)

def categorize_margin(margin):
    if margin < 30:
        return 'Low Margin'
    elif margin < 50:
        return 'Medium Margin'
    else:
        return 'High Margin'

df_products['margin_category'] = df_products['margin_pct'].apply(categorize_margin)

print("  ✓ Created: total_units_sold, total_revenue, margin_category")

# Creating store features
# Store performance metrics
store_metrics = df_transactions.groupby('store_id').agg({
    'total_amount': 'sum',
    'profit': 'sum',
    'transaction_id': 'count',
    'customer_id': 'nunique'
}).reset_index()

store_metrics.columns = ['store_id', 'total_revenue', 'total_profit', 'num_transactions', 'unique_customers']
store_metrics['revenue_per_transaction'] = store_metrics['total_revenue'] / store_metrics['num_transactions']
store_metrics['revenue_per_customer'] = store_metrics['total_revenue'] / store_metrics['unique_customers']

df_stores = df_stores.merge(store_metrics, on='store_id', how='left')

print("  ✓ Created: total_revenue, revenue_per_transaction, unique_customers")

# 4.6 Create Master Analytical Dataset
print("\n4.6 Creating Master Analytical Dataset...")

df_master = df_transactions.merge(df_products[['product_id', 'product_name', 'category', 'margin_category']], 
                                   on='product_id', how='left')
df_master = df_master.merge(df_stores[['store_id', 'store_name', 'region', 'city']], 
                             on='store_id', how='left')
df_master = df_master.merge(df_customers[['customer_id', 'customer_segment', 'lifetime_value']], 
                             on='customer_id', how='left')


# Data Analytics
# Descriptive Statistics
print("\n Transaction Summary Statistics:")
summary_stats = df_transactions[['quantity', 'total_amount', 'profit', 'profit_margin_pct']].describe()
print(summary_stats)

# Revenue Analysis
print("\n Revenue Breakdown:")
print(f"  Total Revenue: ${df_transactions['total_amount'].sum():,.2f}")
print(f"  Total Profit: ${df_transactions['profit'].sum():,.2f}")
print(f"  Average Transaction Value: ${df_transactions['total_amount'].mean():.2f}")
print(f"  Median Transaction Value: ${df_transactions['total_amount'].median():.2f}")
print(f"  Overall Profit Margin: {(df_transactions['profit'].sum() / df_transactions['total_amount'].sum() * 100):.2f}%")

# Time-based Analysis
print("\n Temporal Patterns:")
yearly_sales = df_transactions.groupby('year')['total_amount'].sum()
print("\nRevenue by Year:")
print(yearly_sales)

monthly_avg = df_transactions.groupby('month_name')['total_amount'].mean().sort_values(ascending=False)
print("\nTop 3 Months by Avg Transaction Value:")
print(monthly_avg.head(3))

# Category Performance
print("\n Category Performance:")
category_perf = df_master.groupby('category').agg({
    'total_amount': 'sum',
    'profit': 'sum',
    'transaction_id': 'count'
}).round(2)
category_perf.columns = ['Revenue', 'Profit', 'Transactions']
category_perf = category_perf.sort_values('Revenue', ascending=False)
print(category_perf)

# Regional Performance
print("\n Regional Performance:")
regional_perf = df_master.groupby('region').agg({
    'total_amount': 'sum',
    'profit': 'sum',
    'transaction_id': 'count'
}).round(2)
regional_perf.columns = ['Revenue', 'Profit', 'Transactions']
print(regional_perf)

# Customer Segment Analysis
print("\n Customer Segment Analysis:")
segment_analysis = df_master.groupby('customer_segment').agg({
    'customer_id': 'nunique',
    'total_amount': ['sum', 'mean'],
    'profit': 'sum'
}).round(2)
print(segment_analysis)


# RFM Analysis (Recency, Frequency, Monetary)
print("\n RFM Analysis...")

analysis_date = df_transactions['transaction_date'].max() + pd.Timedelta(days=1)

rfm = df_transactions.groupby('customer_id').agg({
    'transaction_date': lambda x: (analysis_date - x.max()).days,  # Recency
    'transaction_id': 'count',  # Frequency
    'total_amount': 'sum'  # Monetary
}).reset_index()

rfm.columns = ['customer_id', 'recency', 'frequency', 'monetary']

# Create RFM scores
rfm['r_score'] = pd.qcut(rfm['recency'], 4, labels=[4, 3, 2, 1])
rfm['f_score'] = pd.qcut(rfm['frequency'].rank(method='first'), 4, labels=[1, 2, 3, 4])
rfm['m_score'] = pd.qcut(rfm['monetary'], 4, labels=[1, 2, 3, 4])

rfm['rfm_score'] = rfm['r_score'].astype(str) + rfm['f_score'].astype(str) + rfm['m_score'].astype(str)

print("  RFM Segments Distribution:")
print(rfm['rfm_score'].value_counts().head(10))

# Cohort Analysis
print("\n6.2 Cohort Analysis...")

df_transactions['order_month'] = df_transactions['transaction_date'].dt.to_period('M')
df_transactions['cohort'] = df_transactions.groupby('customer_id')['transaction_date'].transform('min').dt.to_period('M')

cohort_data = df_transactions.groupby(['cohort', 'order_month']).agg({
    'customer_id': 'nunique'
}).reset_index()

cohort_data['period_number'] = (cohort_data['order_month'] - cohort_data['cohort']).apply(lambda x: x.n)

cohort_pivot = cohort_data.pivot_table(index='cohort', columns='period_number', values='customer_id')

print("  Cohort retention table created")
print(f"  Cohorts tracked: {len(cohort_pivot)}")

# Product Affinity Analysis
print("\n6.3 Product Basket Analysis...")

# Find products frequently bought together
basket = df_transactions.groupby(['customer_id', 'transaction_date'])['product_id'].apply(list).reset_index()
basket['basket_size'] = basket['product_id'].apply(len)

print(f"  Average basket size: {basket['basket_size'].mean():.2f} items")
print(f"  Transactions with multiple items: {(basket['basket_size'] > 1).sum()}")



