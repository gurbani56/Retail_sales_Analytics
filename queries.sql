SELECT * FROM public.fact_sales
ORDER BY transaction_id ASC LIMIT 100

-- RETAIL ANALYTICS 
-- Database: retail_analytics
-- Purpose: Comprehensive sales analysis queries

-- 1. DATABASE OVERVIEW
-- Shows record counts for all tables

SELECT 
    'Products' as table_name, 
    COUNT(*)::text as record_count 
FROM dim_products
UNION ALL
SELECT 'Stores', COUNT(*)::text FROM dim_stores
UNION ALL
SELECT 'Customers', COUNT(*)::text FROM dim_customers
UNION ALL
SELECT 'Transactions', COUNT(*)::text FROM fact_sales;


-- 2. TOTAL REVENUE & PROFIT
-- Summary of overall business performance
SELECT 
    TO_CHAR(SUM(total_amount), 'FM$999,999,999.00') as total_revenue,
    TO_CHAR(SUM(profit), 'FM$999,999,999.00') as total_profit,
    ROUND(AVG(profit_margin)::numeric, 2) || '%' as avg_margin
FROM fact_sales;

-- 3. TOP 5 PRODUCTS BY REVENUE
-- Identifies best-selling products
SELECT 
    p.product_name,
    p.category,
    TO_CHAR(SUM(s.total_amount), 'FM$999,999,999.00') as revenue,
    SUM(s.quantity) as units_sold
FROM fact_sales s
JOIN dim_products p ON s.product_id = p.product_id
GROUP BY p.product_name, p.category
ORDER BY SUM(s.total_amount) DESC
LIMIT 5;


-- 4. REGIONAL PERFORMANCE
-- Compares sales across different regions
SELECT 
    st.region,
    COUNT(DISTINCT st.store_id) as stores,
    COUNT(s.transaction_id) as transactions,
    TO_CHAR(SUM(s.total_amount), 'FM$999,999,999.00') as revenue,
    TO_CHAR(SUM(s.profit), 'FM$999,999,999.00') as profit,
    ROUND(AVG(s.profit_margin)::numeric, 2) || '%' as avg_margin
FROM fact_sales s
JOIN dim_stores st ON s.store_id = st.store_id
GROUP BY st.region
ORDER BY SUM(s.total_amount) DESC;


-- 5. CUSTOMER SEGMENT ANALYSIS
-- Revenue breakdown by customer segments
SELECT 
    c.customer_segment,
    COUNT(DISTINCT c.customer_id) as customers,
    COUNT(s.transaction_id) as transactions,
    TO_CHAR(SUM(s.total_amount), 'FM$999,999,999.00') as revenue,
    TO_CHAR(AVG(s.total_amount), 'FM$999,999.00') as avg_transaction,
    ROUND(SUM(s.total_amount) * 100.0 / SUM(SUM(s.total_amount)) OVER (), 2) || '%' as revenue_share
FROM dim_customers c
LEFT JOIN fact_sales s ON c.customer_id = s.customer_id
GROUP BY c.customer_segment
ORDER BY SUM(s.total_amount) DESC;


-- 6. MONTHLY SALES TREND
-- Shows revenue and profit trends over time
SELECT 
    TO_CHAR(transaction_date, 'YYYY-MM') as month,
    COUNT(*) as transactions,
    SUM(quantity) as units_sold,
    TO_CHAR(SUM(total_amount), 'FM$999,999,999.00') as revenue,
    TO_CHAR(SUM(profit), 'FM$999,999,999.00') as profit,
    ROUND(AVG(profit_margin)::numeric, 2) || '%' as avg_margin
FROM fact_sales
GROUP BY TO_CHAR(transaction_date, 'YYYY-MM')
ORDER BY month;


-- 7. TOP 10 CUSTOMERS BY REVENUE
-- Identifies highest-value customers
SELECT 
    c.customer_name,
    c.customer_segment,
    c.email,
    COUNT(s.transaction_id) as total_purchases,
    TO_CHAR(SUM(s.total_amount), 'FM$999,999,999.00') as total_spent,
    TO_CHAR(AVG(s.total_amount), 'FM$999,999.00') as avg_purchase,
    TO_CHAR(MAX(s.transaction_date), 'YYYY-MM-DD') as last_purchase
FROM dim_customers c
JOIN fact_sales s ON c.customer_id = s.customer_id
GROUP BY c.customer_name, c.customer_segment, c.email
ORDER BY SUM(s.total_amount) DESC
LIMIT 10;


-- 8. PAYMENT METHOD DISTRIBUTION
-- Shows popularity of different payment methods
SELECT 
    payment_method,
    COUNT(*) as transactions,
    TO_CHAR(SUM(total_amount), 'FM$999,999,999.00') as revenue,
    TO_CHAR(AVG(total_amount), 'FM$999,999.00') as avg_transaction,
    ROUND(COUNT(*)::numeric * 100 / SUM(COUNT(*)) OVER (), 2) || '%' as transaction_percentage,
    ROUND(SUM(total_amount) * 100.0 / SUM(SUM(total_amount)) OVER (), 2) || '%' as revenue_percentage
FROM fact_sales
GROUP BY payment_method
ORDER BY COUNT(*) DESC;


-- 9. CATEGORY PERFORMANCE
-- Revenue and profitability by product category

SELECT 
    p.category,
    COUNT(DISTINCT p.product_id) as products,
    COUNT(s.transaction_id) as transactions,
    SUM(s.quantity) as units_sold,
    TO_CHAR(SUM(s.total_amount), 'FM$999,999,999.00') as revenue,
    TO_CHAR(SUM(s.profit), 'FM$999,999,999.00') as profit,
    ROUND(AVG(s.profit_margin)::numeric, 2) || '%' as avg_margin,
    ROUND(SUM(s.total_amount) * 100.0 / SUM(SUM(s.total_amount)) OVER (), 2) || '%' as revenue_share
FROM fact_sales s
JOIN dim_products p ON s.product_id = p.product_id
GROUP BY p.category
ORDER BY SUM(s.total_amount) DESC;


-- 10. STORE PERFORMANCE RANKING
-- Ranks stores by revenue with detailed metrics
SELECT 
    st.store_name,
    st.city,
    st.region,
    st.state,
    COUNT(s.transaction_id) as transactions,
    SUM(s.quantity) as units_sold,
    TO_CHAR(SUM(s.total_amount), 'FM$999,999,999.00') as revenue,
    TO_CHAR(SUM(s.profit), 'FM$999,999,999.00') as profit,
    TO_CHAR(AVG(s.total_amount), 'FM$999,999.00') as avg_transaction,
    ROUND(AVG(s.profit_margin)::numeric, 2) || '%' as avg_margin,
    RANK() OVER (ORDER BY SUM(s.total_amount) DESC) as revenue_rank
FROM fact_sales s
JOIN dim_stores st ON s.store_id = st.store_id
GROUP BY st.store_name, st.city, st.region, st.state
ORDER BY SUM(s.total_amount) DESC;


-- 11. PRODUCT PROFITABILITY ANALYSIS
-- Shows most and least profitable products

SELECT 
    p.product_name,
    p.category,
    COUNT(s.transaction_id) as transactions,
    SUM(s.quantity) as units_sold,
    TO_CHAR(AVG(s.unit_price), 'FM$999,999.00') as avg_price,
    TO_CHAR(SUM(s.total_amount), 'FM$999,999,999.00') as revenue,
    TO_CHAR(SUM(s.profit), 'FM$999,999,999.00') as profit,
    ROUND(AVG(s.profit_margin)::numeric, 2) || '%' as avg_margin,
    RANK() OVER (ORDER BY SUM(s.profit) DESC) as profit_rank
FROM fact_sales s
JOIN dim_products p ON s.product_id = p.product_id
GROUP BY p.product_name, p.category
ORDER BY SUM(s.profit) DESC
LIMIT 20;


-- 12. DAILY SALES PATTERN
-- Shows which days of the week perform best
SELECT 
    TO_CHAR(transaction_date, 'Day') as day_of_week,
    EXTRACT(DOW FROM transaction_date) as day_number,
    COUNT(*) as transactions,
    SUM(quantity) as units_sold,
    TO_CHAR(SUM(total_amount), 'FM$999,999,999.00') as revenue,
    TO_CHAR(AVG(total_amount), 'FM$999,999.00') as avg_transaction
FROM fact_sales
GROUP BY TO_CHAR(transaction_date, 'Day'), EXTRACT(DOW FROM transaction_date)
ORDER BY day_number;


-- 13. CUSTOMER LIFETIME VALUE (CLV)
-- Analyzes customer purchase patterns
SELECT 
    c.customer_segment,
    COUNT(DISTINCT c.customer_id) as customers,
    TO_CHAR(AVG(customer_stats.total_spent), 'FM$999,999.00') as avg_lifetime_value,
    ROUND(AVG(customer_stats.purchase_count), 2) as avg_purchases_per_customer,
    TO_CHAR(AVG(customer_stats.avg_transaction), 'FM$999,999.00') as avg_transaction_value,
    ROUND(AVG(customer_stats.days_active), 0) as avg_days_active
FROM dim_customers c
JOIN (
    SELECT 
        customer_id,
        SUM(total_amount) as total_spent,
        COUNT(*) as purchase_count,
        AVG(total_amount) as avg_transaction,
        MAX(transaction_date) - MIN(transaction_date) as days_active
    FROM fact_sales
    GROUP BY customer_id
) customer_stats ON c.customer_id = customer_stats.customer_id
GROUP BY c.customer_segment
ORDER BY AVG(customer_stats.total_spent) DESC;


-- 14. INVENTORY TURNOVER BY CATEGORY
-- Shows how quickly products sell by category

SELECT 
    p.category,
    COUNT(DISTINCT p.product_id) as unique_products,
    SUM(s.quantity) as total_units_sold,
    TO_CHAR(SUM(s.total_amount), 'FM$999,999,999.00') as revenue,
    ROUND(SUM(s.quantity)::numeric / COUNT(DISTINCT p.product_id), 2) as avg_units_per_product,
    ROUND(SUM(s.total_amount) / COUNT(DISTINCT p.product_id), 2) as avg_revenue_per_product
FROM fact_sales s
JOIN dim_products p ON s.product_id = p.product_id
GROUP BY p.category
ORDER BY SUM(s.quantity) DESC;


-- 15. YEAR-OVER-YEAR GROWTH (if multi-year data exists)
-- Compares performance across years

SELECT 
    EXTRACT(YEAR FROM transaction_date) as year,
    COUNT(*) as transactions,
    SUM(quantity) as units_sold,
    TO_CHAR(SUM(total_amount), 'FM$999,999,999.00') as revenue,
    TO_CHAR(SUM(profit), 'FM$999,999,999.00') as profit,
    ROUND(AVG(profit_margin)::numeric, 2) || '%' as avg_margin
FROM fact_sales
GROUP BY EXTRACT(YEAR FROM transaction_date)
ORDER BY year;


-- 16. STORE AGE vs PERFORMANCE
-- Analyzes if older stores perform better

SELECT 
    st.store_name,
    st.opened_date,
    EXTRACT(YEAR FROM AGE(CURRENT_DATE, st.opened_date)) as store_age_years,
    COUNT(s.transaction_id) as transactions,
    TO_CHAR(SUM(s.total_amount), 'FM$999,999,999.00') as revenue,
    TO_CHAR(SUM(s.profit), 'FM$999,999,999.00') as profit
FROM dim_stores st
LEFT JOIN fact_sales s ON st.store_id = s.store_id
GROUP BY st.store_name, st.opened_date
ORDER BY EXTRACT(YEAR FROM AGE(CURRENT_DATE, st.opened_date)) DESC;


-- 17. CROSS-SELLING OPPORTUNITIES
-- Products frequently bought together

SELECT 
    p1.product_name as product_1,
    p2.product_name as product_2,
    COUNT(*) as times_bought_together,
    TO_CHAR(SUM(s1.total_amount + s2.total_amount), 'FM$999,999,999.00') as combined_revenue
FROM fact_sales s1
JOIN fact_sales s2 ON s1.customer_id = s2.customer_id 
    AND s1.transaction_date = s2.transaction_date
    AND s1.product_id < s2.product_id
JOIN dim_products p1 ON s1.product_id = p1.product_id
JOIN dim_products p2 ON s2.product_id = p2.product_id
GROUP BY p1.product_name, p2.product_name
ORDER BY COUNT(*) DESC
LIMIT 20;

