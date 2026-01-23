import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Set visualization style
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")
plt.rcParams['figure.figsize'] = (15, 8)
plt.rcParams['font.size'] = 10

# Load cleaned data
print("Loading cleaned datasets...")
df_transactions = pd.read_csv('transactions_cleaned.csv')
df_master = pd.read_csv('master_dataset.csv')
df_products = pd.read_csv('products_cleaned.csv')
df_customers = pd.read_csv('customers_cleaned.csv')
df_stores = pd.read_csv('stores_cleaned.csv')

# Convert date columns
df_transactions['transaction_date'] = pd.to_datetime(df_transactions['transaction_date'])
df_master['transaction_date'] = pd.to_datetime(df_master['transaction_date'])

print("Data loaded successfully!")

# Create output directory for plots
import os
if not os.path.exists('visualizations'):
    os.makedirs('visualizations')

# VISUALIZATION 1
print("\nCreating Visualization 1: Revenue & Profit Trends...")

fig, axes = plt.subplots(2, 2, figsize=(18, 12))
fig.suptitle('Revenue & Profit Analysis Over Time', fontsize=16, fontweight='bold')

# Monthly Revenue Trend
monthly_data = df_transactions.groupby(df_transactions['transaction_date'].dt.to_period('M')).agg({
    'total_amount': 'sum',
    'profit': 'sum',
    'transaction_id': 'count'
}).reset_index()
monthly_data['transaction_date'] = monthly_data['transaction_date'].dt.to_timestamp()

axes[0, 0].plot(monthly_data['transaction_date'], monthly_data['total_amount'], 
                marker='o', linewidth=2, label='Revenue')
axes[0, 0].plot(monthly_data['transaction_date'], monthly_data['profit'], 
                marker='s', linewidth=2, label='Profit')
axes[0, 0].set_title('Monthly Revenue & Profit Trends', fontweight='bold')
axes[0, 0].set_xlabel('Month')
axes[0, 0].set_ylabel('Amount ($)')
axes[0, 0].legend()
axes[0, 0].grid(True, alpha=0.3)
axes[0, 0].tick_params(axis='x', rotation=45)

# Quarterly Performance
quarterly_data = df_transactions.groupby(['year', 'quarter']).agg({
    'total_amount': 'sum',
    'profit': 'sum'
}).reset_index()
quarterly_data['quarter_label'] = (quarterly_data['year'].astype(str) + '-Q' + 
                                    quarterly_data['quarter'].astype(str))

x = range(len(quarterly_data))
width = 0.35
axes[0, 1].bar([i - width/2 for i in x], quarterly_data['total_amount'], 
               width, label='Revenue', alpha=0.8)
axes[0, 1].bar([i + width/2 for i in x], quarterly_data['profit'], 
               width, label='Profit', alpha=0.8)
axes[0, 1].set_title('Quarterly Revenue & Profit Comparison', fontweight='bold')
axes[0, 1].set_xlabel('Quarter')
axes[0, 1].set_ylabel('Amount ($)')
axes[0, 1].set_xticks(x)
axes[0, 1].set_xticklabels(quarterly_data['quarter_label'], rotation=45)
axes[0, 1].legend()
axes[0, 1].grid(True, alpha=0.3, axis='y')

# Day of Week Analysis
dow_data = df_transactions.groupby('day_name').agg({
    'total_amount': 'sum',
    'transaction_id': 'count'
}).reindex(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])

axes[1, 0].bar(dow_data.index, dow_data['total_amount'], color='steelblue', alpha=0.7)
axes[1, 0].set_title('Revenue by Day of Week', fontweight='bold')
axes[1, 0].set_xlabel('Day of Week')
axes[1, 0].set_ylabel('Total Revenue ($)')
axes[1, 0].tick_params(axis='x', rotation=45)
axes[1, 0].grid(True, alpha=0.3, axis='y')

# Seasonal Analysis
seasonal_data = df_transactions.groupby('season').agg({
    'total_amount': 'sum',
    'profit': 'sum'
}).reindex(['Spring', 'Summer', 'Fall', 'Winter'])

axes[1, 1].barh(seasonal_data.index, seasonal_data['total_amount'], 
                color='coral', alpha=0.7)
axes[1, 1].set_title('Revenue by Season', fontweight='bold')
axes[1, 1].set_xlabel('Total Revenue ($)')
axes[1, 1].set_ylabel('Season')
axes[1, 1].grid(True, alpha=0.3, axis='x')

plt.tight_layout()
plt.savefig('visualizations/1_revenue_profit_trends.png', dpi=300, bbox_inches='tight')
print("✓ Saved: 1_revenue_profit_trends.png")


# VISUALIZATION 2: Product & Category Performance
print("Creating Visualization 2: Product & Category Performance...")

fig, axes = plt.subplots(2, 2, figsize=(18, 12))
fig.suptitle('Product & Category Performance Analysis', fontsize=16, fontweight='bold')

# Category Revenue
category_data = df_master.groupby('category').agg({
    'total_amount': 'sum',
    'profit': 'sum',
    'transaction_id': 'count'
}).sort_values('total_amount', ascending=True)

axes[0, 0].barh(category_data.index, category_data['total_amount'], 
                color='teal', alpha=0.7)
axes[0, 0].set_title('Revenue by Category', fontweight='bold')
axes[0, 0].set_xlabel('Total Revenue ($)')
axes[0, 0].grid(True, alpha=0.3, axis='x')

# Category Profit Margin
category_margin = df_master.groupby('category').agg({
    'profit': 'sum',
    'total_amount': 'sum'
})
category_margin['profit_margin'] = (
    (category_margin['profit'] / category_margin['total_amount'] * 100).round(2)
)
category_margin = category_margin.sort_values('profit_margin', ascending=False)

colors = ['green' if x > 40 else 'orange' if x > 30 else 'red' 
          for x in category_margin['profit_margin']]
axes[0, 1].bar(category_margin.index, category_margin['profit_margin'], 
               color=colors, alpha=0.7)
axes[0, 1].set_title('Profit Margin by Category (%)', fontweight='bold')
axes[0, 1].set_ylabel('Profit Margin (%)')
axes[0, 1].tick_params(axis='x', rotation=45)
axes[0, 1].axhline(y=35, color='red', linestyle='--', alpha=0.5, label='Target: 35%')
axes[0, 1].legend()
axes[0, 1].grid(True, alpha=0.3, axis='y')

# Top 10 Products
top_products = df_master.groupby('product_name')['total_amount'].sum().nlargest(10).sort_values()

axes[1, 0].barh(top_products.index, top_products.values, color='purple', alpha=0.7)
axes[1, 0].set_title('Top 10 Products by Revenue', fontweight='bold')
axes[1, 0].set_xlabel('Total Revenue ($)')
axes[1, 0].grid(True, alpha=0.3, axis='x')

# Product Price vs Profit Scatter
product_summary = df_master.groupby('product_name').agg({
    'unit_price': 'mean',
    'profit': 'sum',
    'quantity': 'sum'
}).reset_index()

scatter = axes[1, 1].scatter(product_summary['unit_price'], 
                            product_summary['profit'], 
                            s=product_summary['quantity']*2, 
                            alpha=0.6, 
                            c=product_summary['quantity'], 
                            cmap='viridis')
axes[1, 1].set_title('Product Price vs Total Profit (Size = Quantity Sold)', 
                     fontweight='bold')
axes[1, 1].set_xlabel('Average Unit Price ($)')
axes[1, 1].set_ylabel('Total Profit ($)')
axes[1, 1].grid(True, alpha=0.3)
plt.colorbar(scatter, ax=axes[1, 1], label='Quantity Sold')

plt.tight_layout()
plt.savefig('visualizations/2_product_category_analysis.png', dpi=300, bbox_inches='tight')
print("✓ Saved: 2_product_category_analysis.png")


# VISUALIZATION 3: Geographic & Store Performance
print("Creating Visualization 3: Geographic & Store Performance...")

fig, axes = plt.subplots(2, 2, figsize=(18, 12))
fig.suptitle('Geographic & Store Performance Analysis', fontsize=16, fontweight='bold')

# Regional Revenue
regional_data = df_master.groupby('region').agg({
    'total_amount': 'sum',
    'profit': 'sum',
    'transaction_id': 'count'
}).sort_values('total_amount', ascending=False)

axes[0, 0].bar(regional_data.index, regional_data['total_amount'], 
               color='skyblue', alpha=0.7, label='Revenue')
axes[0, 0].set_title('Revenue by Region', fontweight='bold')
axes[0, 0].set_ylabel('Total Revenue ($)')
axes[0, 0].tick_params(axis='x', rotation=45)
axes[0, 0].grid(True, alpha=0.3, axis='y')

# Store Performance - Top 10
store_performance = df_master.groupby('store_name').agg({
    'total_amount': 'sum',
    'profit': 'sum'
}).nlargest(10, 'total_amount')

x = range(len(store_performance))
width = 0.35
axes[0, 1].bar([i - width/2 for i in x], store_performance['total_amount'], 
               width, label='Revenue', alpha=0.8)
axes[0, 1].bar([i + width/2 for i in x], store_performance['profit'], 
               width, label='Profit', alpha=0.8)
axes[0, 1].set_title('Top 10 Stores - Revenue & Profit', fontweight='bold')
axes[0, 1].set_ylabel('Amount ($)')
axes[0, 1].set_xticks(x)
axes[0, 1].set_xticklabels(store_performance.index, rotation=45, ha='right')
axes[0, 1].legend()
axes[0, 1].grid(True, alpha=0.3, axis='y')

# Regional Transaction Distribution
regional_trans = df_master.groupby('region')['transaction_id'].count()
axes[1, 0].pie(regional_trans, labels=regional_trans.index, 
               autopct='%1.1f%%', startangle=90)
axes[1, 0].set_title('Transaction Distribution by Region', fontweight='bold')

# Region-Category Heatmap
region_category = df_master.groupby(['region', 'category'])['total_amount'].sum().unstack(fill_value=0)
sns.heatmap(region_category, annot=True, fmt='.0f', cmap='YlOrRd', 
            ax=axes[1, 1], cbar_kws={'label': 'Revenue ($)'})
axes[1, 1].set_title('Revenue Heatmap: Region vs Category', fontweight='bold')
axes[1, 1].set_ylabel('Region')
axes[1, 1].set_xlabel('Category')

plt.tight_layout()
plt.savefig('visualizations/3_geographic_store_analysis.png', dpi=300, bbox_inches='tight')
print("✓ Saved: 3_geographic_store_analysis.png")


# VISUALIZATION 4: Customer Analysis
print("Creating Visualization 4: Customer Analysis...")

fig, axes = plt.subplots(2, 2, figsize=(18, 12))
fig.suptitle('Customer Behavior & Segmentation Analysis', fontsize=16, fontweight='bold')

# Customer Segment Revenue
segment_data = df_master.groupby('customer_segment').agg({
    'total_amount': 'sum',
    'customer_id': 'nunique',
    'transaction_id': 'count'
}).sort_values('total_amount', ascending=False)

axes[0, 0].bar(segment_data.index, segment_data['total_amount'], 
               color='mediumseagreen', alpha=0.7)
axes[0, 0].set_title('Revenue by Customer Segment', fontweight='bold')
axes[0, 0].set_ylabel('Total Revenue ($)')
axes[0, 0].grid(True, alpha=0.3, axis='y')

# Customer Segment Distribution
axes[0, 1].pie(segment_data['customer_id'], labels=segment_data.index, 
               autopct='%1.1f%%', startangle=90)
axes[0, 1].set_title('Customer Distribution by Segment', fontweight='bold')

# Transaction Size Distribution
transaction_size_order = ['Small', 'Medium', 'Large', 'Very Large']
size_data = df_transactions.groupby('transaction_size')['transaction_id'].count().reindex(transaction_size_order)

axes[1, 0].bar(size_data.index, size_data.values, color='indianred', alpha=0.7)
axes[1, 0].set_title('Transaction Count by Size', fontweight='bold')
axes[1, 0].set_ylabel('Number of Transactions')
axes[1, 0].tick_params(axis='x', rotation=45)
axes[1, 0].grid(True, alpha=0.3, axis='y')

# Customer Lifetime Value Distribution
axes[1, 1].hist(df_customers['lifetime_value'].dropna(), bins=30, 
                color='gold', alpha=0.7, edgecolor='black')
axes[1, 1].set_title('Customer Lifetime Value Distribution', fontweight='bold')
axes[1, 1].set_xlabel('Lifetime Value ($)')
axes[1, 1].set_ylabel('Number of Customers')
median_ltv = df_customers['lifetime_value'].median()
axes[1, 1].axvline(median_ltv, color='red', linestyle='--', 
                   label=f"Median: ${median_ltv:.2f}")
axes[1, 1].legend()
axes[1, 1].grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig('visualizations/4_customer_analysis.png', dpi=300, bbox_inches='tight')
print("✓ Saved: 4_customer_analysis.png")

# VISUALIZATION 5: Discount & Profitability Analysis

print("Creating Visualization 5: Discount & Profitability Analysis...")

fig, axes = plt.subplots(2, 2, figsize=(18, 12))
fig.suptitle('Discount Impact & Profitability Analysis', fontsize=16, fontweight='bold')

# Discount Distribution
discount_bins = [0, 5, 10, 15, 20, 30, 100]
discount_labels = ['No Discount', '1-5%', '6-10%', '11-15%', '16-20%', '21%+']
df_transactions['discount_range'] = pd.cut(df_transactions['discount_pct'], 
                                            bins=discount_bins, 
                                            labels=discount_labels, 
                                            include_lowest=True)

discount_dist = df_transactions['discount_range'].value_counts().sort_index()
axes[0, 0].bar(range(len(discount_dist)), discount_dist.values, 
               color='salmon', alpha=0.7)
axes[0, 0].set_title('Transaction Distribution by Discount Range', fontweight='bold')
axes[0, 0].set_ylabel('Number of Transactions')
axes[0, 0].set_xticks(range(len(discount_dist)))
axes[0, 0].set_xticklabels(discount_dist.index, rotation=45)
axes[0, 0].grid(True, alpha=0.3, axis='y')

# Discount vs Profit Margin
discount_impact = df_transactions.groupby('discount_range').agg({
    'profit_margin_pct': 'mean',
    'total_amount': 'sum',
    'transaction_id': 'count'
})

axes[0, 1].plot(discount_impact.index, discount_impact['profit_margin_pct'], 
                marker='o', linewidth=2, color='darkred')
axes[0, 1].set_title('Average Profit Margin by Discount Range', fontweight='bold')
axes[0, 1].set_ylabel('Average Profit Margin (%)')
axes[0, 1].tick_params(axis='x', rotation=45)
axes[0, 1].grid(True, alpha=0.3)
avg_margin = df_transactions['profit_margin_pct'].mean()
axes[0, 1].axhline(y=avg_margin, color='green', linestyle='--', 
                   alpha=0.5, label='Overall Average')
axes[0, 1].legend()

# Payment Method Analysis
payment_data = df_transactions.groupby('payment_method').agg({
    'total_amount': 'sum',
    'transaction_id': 'count'
}).sort_values('total_amount', ascending=True)

axes[1, 0].barh(payment_data.index, payment_data['total_amount'], 
                color='lightcoral', alpha=0.7)
axes[1, 0].set_title('Revenue by Payment Method', fontweight='bold')
axes[1, 0].set_xlabel('Total Revenue ($)')
axes[1, 0].grid(True, alpha=0.3, axis='x')

# Profit Margin Distribution
axes[1, 1].hist(df_transactions['profit_margin_pct'], bins=50, 
                color='seagreen', alpha=0.7, edgecolor='black')
axes[1, 1].set_title('Profit Margin Distribution', fontweight='bold')
axes[1, 1].set_xlabel('Profit Margin (%)')
axes[1, 1].set_ylabel('Frequency')
mean_margin = df_transactions['profit_margin_pct'].mean()
axes[1, 1].axvline(mean_margin, color='red', linestyle='--', linewidth=2, 
                   label=f"Mean: {mean_margin:.2f}%")
axes[1, 1].legend()
axes[1, 1].grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig('visualizations/5_discount_profitability_analysis.png', dpi=300, bbox_inches='tight')
print("✓ Saved: 5_discount_profitability_analysis.png")


plt.show()