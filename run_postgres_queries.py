import psycopg2
import pandas as pd
from tabulate import tabulate

# =====================================================
# DATABASE CONNECTION
# =====================================================

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'user': 'postgres',
    'password': 'your_password',  # CHANGE THIS!
    'database': 'retail_analytics'
}

print("=" * 80)
print("RETAIL ANALYTICS - SQL QUERY RESULTS (PostgreSQL)")
print("=" * 80)

try:
    conn = psycopg2.connect(**DB_CONFIG)
    print("✓ Connected to PostgreSQL database\n")
except psycopg2.Error as e:
    print(f"✗ Connection error: {e}")
    exit(1)

# =====================================================
# QUERY 1: Monthly Revenue and Profit Trends
# =====================================================

print("\n1. MONTHLY REVENUE AND PROFIT TRENDS")
print("-" * 80)

query1 = '''
SELECT 
    EXTRACT(YEAR FROM transaction_date)::INTEGER AS year,
    EXTRACT(MONTH FROM transaction_date)::INTEGER AS month,
    COUNT(DISTINCT transaction_id) AS total_transactions,
    SUM(quantity) AS total_units_sold,
    ROUND(SUM(total_amount)::numeric, 2) AS total_revenue,
    ROUND(SUM(profit)::numeric, 2) AS total_profit,
    ROUND(AVG(profit_margin)::numeric, 2) AS avg_profit_margin,
    ROUND(SUM(discount_amount)::numeric, 2) AS total_discounts
FROM fact_sales
GROUP BY year, month
ORDER BY year, month
LIMIT 12
'''

df1 = pd.read_sql_query(query1, conn)
print(tabulate(df1, headers='keys', tablefmt='psql', showindex=False))

# =====================================================
# QUERY 2: Top 10 Products by Revenue
# =====================================================

print("\n\n2. TOP 10 PRODUCTS BY REVENUE AND PROFIT")
print("-" * 80)

query2 = '''
SELECT 
    p.product_name,
    p.category,
    COUNT(s.transaction_id) AS num_sales,
    SUM(s.quantity) AS units_sold,
    ROUND(SUM(s.total_amount)::numeric, 2) AS total_revenue,
    ROUND(SUM(s.profit)::numeric, 2) AS total_profit,
    ROUND(AVG(s.profit_margin)::numeric, 2) AS avg_margin_pct
FROM fact_sales s
JOIN dim_products p ON s.product_id = p.product_id
GROUP BY p.product_id, p.product_name, p.category
ORDER BY total_revenue DESC
LIMIT 10
'''

df2 = pd.read_sql_query(query2, conn)
print(tabulate(df2, headers='keys', tablefmt='psql', showindex=False))

# =====================================================
# QUERY 3: Store Performance Analysis
# =====================================================

print("\n\n3. STORE PERFORMANCE RANKING")
print("-" * 80)

query3 = '''
SELECT 
    st.store_name,
    st.region,
    COUNT(DISTINCT s.transaction_id) AS num_transactions,
    COUNT(DISTINCT s.customer_id) AS unique_customers,
    ROUND(SUM(s.total_amount)::numeric, 2) AS total_revenue,
    ROUND(SUM(s.profit)::numeric, 2) AS total_profit,
    ROUND((SUM(s.profit) * 100.0 / NULLIF(SUM(s.total_amount), 0))::numeric, 2) AS profit_margin_pct
FROM fact_sales s
JOIN dim_stores st ON s.store_id = st.store_id
GROUP BY st.store_id, st.store_name, st.region
ORDER BY total_revenue DESC
LIMIT 10
'''

df3 = pd.read_sql_query(query3, conn)
print(tabulate(df3, headers='keys', tablefmt='psql', showindex=False))

# =====================================================
# QUERY 4: Customer Segmentation Analysis
# =====================================================

print("\n\n4. CUSTOMER SEGMENTATION PERFORMANCE")
print("-" * 80)

query4 = '''
SELECT 
    c.customer_segment,
    COUNT(DISTINCT c.customer_id) AS num_customers,
    COUNT(s.transaction_id) AS total_transactions,
    ROUND((COUNT(s.transaction_id)::numeric / NULLIF(COUNT(DISTINCT c.customer_id), 0)), 2) AS avg_trans_per_customer,
    ROUND(SUM(s.total_amount)::numeric, 2) AS total_revenue,
    ROUND((SUM(s.total_amount) / NULLIF(COUNT(DISTINCT c.customer_id), 0))::numeric, 2) AS revenue_per_customer
FROM dim_customers c
LEFT JOIN fact_sales s ON c.customer_id = s.customer_id
GROUP BY c.customer_segment
ORDER BY total_revenue DESC
'''

df4 = pd.read_sql_query(query4, conn)
print(tabulate(df4, headers='keys', tablefmt='psql', showindex=False))

# =====================================================
# QUERY 5: Category Performance with Market Share
# =====================================================

print("\n\n5. CATEGORY PERFORMANCE & MARKET SHARE")
print("-" * 80)

query5 = '''
SELECT 
    p.category,
    COUNT(s.transaction_id) AS num_transactions,
    SUM(s.quantity) AS units_sold,
    ROUND(SUM(s.total_amount)::numeric, 2) AS total_revenue,
    ROUND(SUM(s.profit)::numeric, 2) AS total_profit,
    ROUND(AVG(s.profit_margin)::numeric, 2) AS avg_profit_margin,
    ROUND((SUM(s.total_amount) * 100.0 / (SELECT SUM(total_amount) FROM fact_sales))::numeric, 2) AS revenue_share_pct
FROM fact_sales s
JOIN dim_products p ON s.product_id = p.product_id
GROUP BY p.category
ORDER BY total_revenue DESC
'''

df5 = pd.read_sql_query(query5, conn)
print(tabulate(df5, headers='keys', tablefmt='psql', showindex=False))

# =====================================================
# QUERY 6: Regional Performance Comparison
# =====================================================

print("\n\n6. REGIONAL SALES COMPARISON")
print("-" * 80)

query6 = '''
SELECT 
    st.region,
    COUNT(DISTINCT st.store_id) AS num_stores,
    ROUND(SUM(s.total_amount)::numeric, 2) AS total_revenue,
    ROUND(SUM(s.profit)::numeric, 2) AS total_profit,
    ROUND((SUM(s.total_amount) / NULLIF(COUNT(DISTINCT st.store_id), 0))::numeric, 2) AS avg_revenue_per_store,
    ROUND((SUM(s.profit) / NULLIF(COUNT(DISTINCT st.store_id), 0))::numeric, 2) AS avg_profit_per_store,
    ROUND((SUM(s.profit) * 100.0 / NULLIF(SUM(s.total_amount), 0))::numeric, 2) AS profit_margin_pct
FROM fact_sales s
JOIN dim_stores st ON s.store_id = st.store_id
GROUP BY st.region
ORDER BY total_revenue DESC
'''

df6 = pd.read_sql_query(query6, conn)
print(tabulate(df6, headers='keys', tablefmt='psql', showindex=False))

# =====================================================
# QUERY 7: Payment Method Distribution
# =====================================================

print("\n\n7. PAYMENT METHOD ANALYSIS")
print("-" * 80)

query7 = '''
SELECT 
    payment_method,
    COUNT(*) AS num_transactions,
    ROUND(SUM(total_amount)::numeric, 2) AS total_revenue,
    ROUND(AVG(total_amount)::numeric, 2) AS avg_transaction_value,
    ROUND((SUM(total_amount) * 100.0 / (SELECT SUM(total_amount) FROM fact_sales))::numeric, 2) AS revenue_share_pct
FROM fact_sales
GROUP BY payment_method
ORDER BY total_revenue DESC
'''

df7 = pd.read_sql_query(query7, conn)
print(tabulate(df7, headers='keys', tablefmt='psql', showindex=False))

# =====================================================
# QUERY 8: Discount Impact on Profitability
# =====================================================

print("\n\n8. DISCOUNT IMPACT ON PROFITABILITY")
print("-" * 80)

query8 = '''
SELECT 
    CASE 
        WHEN discount_pct = 0 THEN 'No Discount'
        WHEN discount_pct <= 10 THEN '1-10%'
        WHEN discount_pct <= 20 THEN '11-20%'
        ELSE '21%+'
    END AS discount_range,
    COUNT(*) AS num_transactions,
    ROUND(SUM(total_amount)::numeric, 2) AS total_revenue,
    ROUND(SUM(profit)::numeric, 2) AS total_profit,
    ROUND(AVG(profit_margin)::numeric, 2) AS avg_profit_margin,
    ROUND(SUM(discount_amount)::numeric, 2) AS total_discount_given
FROM fact_sales
GROUP BY discount_range
ORDER BY 
    CASE discount_range
        WHEN 'No Discount' THEN 1
        WHEN '1-10%' THEN 2
        WHEN '11-20%' THEN 3
        ELSE 4
    END
'''

df8 = pd.read_sql_query(query8, conn)
print(tabulate(df8, headers='keys', tablefmt='psql', showindex=False))

# =====================================================
# QUERY 9: Year-over-Year Growth Analysis
# =====================================================

print("\n\n9. YEAR-OVER-YEAR GROWTH BY CATEGORY")
print("-" * 80)

query9 = '''
WITH category_yearly AS (
    SELECT 
        EXTRACT(YEAR FROM s.transaction_date)::INTEGER AS year,
        p.category,
        SUM(s.total_amount) AS revenue
    FROM fact_sales s
    JOIN dim_products p ON s.product_id = p.product_id
    GROUP BY year, p.category
)
SELECT 
    curr.category,
    curr.year,
    ROUND(curr.revenue::numeric, 2) AS current_year_revenue,
    ROUND(prev.revenue::numeric, 2) AS previous_year_revenue,
    ROUND(((curr.revenue - prev.revenue) / NULLIF(prev.revenue, 0) * 100)::numeric, 2) AS yoy_growth_pct
FROM category_yearly curr
LEFT JOIN category_yearly prev 
    ON curr.category = prev.category 
    AND curr.year = prev.year + 1
WHERE prev.revenue IS NOT NULL
ORDER BY curr.year DESC, yoy_growth_pct DESC
LIMIT 15
'''

df9 = pd.read_sql_query(query9, conn)
print(tabulate(df9, headers='keys', tablefmt='psql', showindex=False))

# =====================================================
# QUERY 10: Top Customers Analysis
# =====================================================

print("\n\n10. TOP 10 CUSTOMERS BY REVENUE")
print("-" * 80)

query10 = '''
SELECT 
    c.customer_name,
    c.customer_segment,
    COUNT(s.transaction_id) AS total_purchases,
    SUM(s.quantity) AS total_items,
    ROUND(SUM(s.total_amount)::numeric, 2) AS total_spent,
    ROUND(AVG(s.total_amount)::numeric, 2) AS avg_order_value
FROM fact_sales s
JOIN dim_customers c ON s.customer_id = c.customer_id
GROUP BY c.customer_id, c.customer_name, c.customer_segment
ORDER BY total_spent DESC
LIMIT 10
'''

df10 = pd.read_sql_query(query10, conn)
print(tabulate(df10, headers='keys', tablefmt='psql', showindex=False))

# =====================================================
# EXPORT ALL RESULTS TO CSV
# =====================================================

print("\n\n" + "=" * 80)
print("EXPORTING QUERY RESULTS TO CSV FILES")
print("=" * 80)

df1.to_csv('pg_query_monthly_trends.csv', index=False)
print("✓ pg_query_monthly_trends.csv")

df2.to_csv('pg_query_top_products.csv', index=False)
print("✓ pg_query_top_products.csv")

df3.to_csv('pg_query_store_performance.csv', index=False)
print("✓ pg_query_store_performance.csv")

df4.to_csv('pg_query_customer_segments.csv', index=False)
print("✓ pg_query_customer_segments.csv")

df5.to_csv('pg_query_category_performance.csv', index=False)
print("✓ pg_query_category_performance.csv")

df6.to_csv('pg_query_regional_performance.csv', index=False)
print("✓ pg_query_regional_performance.csv")

df7.to_csv('pg_query_payment_methods.csv', index=False)
print("✓ pg_query_payment_methods.csv")

df8.to_csv('pg_query_discount_impact.csv', index=False)
print("✓ pg_query_discount_impact.csv")

df9.to_csv('pg_query_yoy_growth.csv', index=False)
print("✓ pg_query_yoy_growth.csv")

df10.to_csv('pg_query_top_customers.csv', index=False)
print("✓ pg_query_top_customers.csv")

# Close connection
conn.close()

print("\n" + "=" * 80)
print("✓ ALL QUERIES EXECUTED SUCCESSFULLY!")
print("=" * 80)
print("\nKey Insights Ready for Power BI Dashboard!")
