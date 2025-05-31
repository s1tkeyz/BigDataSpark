-- Используем базу данных с витринами
USE sales_marts;

-- 1. Топ-10 самых продаваемых товаров по количеству
SELECT 
    product_name,
    category,
    total_quantity,
    total_revenue,
    rating
FROM product_sales_mart
ORDER BY total_quantity DESC
LIMIT 10;

-- 2. Топ-10 самых прибыльных товаров
SELECT 
    product_name,
    category,
    total_revenue,
    total_quantity,
    avg_price,
    rating
FROM product_sales_mart
ORDER BY total_revenue DESC
LIMIT 10;

-- 3. Анализ продаж по месяцам с динамикой роста
SELECT 
    year,
    month_name,
    total_revenue,
    orders_count,
    avg_order_size,
    prev_month_revenue,
    revenue_growth_percent
FROM time_sales_mart
ORDER BY year, month;

-- 4. Топ-5 магазинов по обороту
SELECT 
    store_name,
    city,
    country,
    total_revenue,
    orders_count,
    avg_check
FROM store_sales_mart
ORDER BY total_revenue DESC
LIMIT 5;

-- 5. Анализ корреляции рейтинга товаров с продажами
SELECT 
    category,
    product_name,
    rating,
    reviews_count,
    sales_count,
    total_revenue
FROM product_quality_mart
WHERE reviews_count > 10  -- Исключаем товары с малым количеством отзывов
ORDER BY rating DESC, sales_count DESC;

-- 6. Топ-10 клиентов по сумме покупок
SELECT 
    first_name,
    last_name,
    country,
    total_purchases,
    orders_count,
    avg_check
FROM customer_sales_mart
ORDER BY total_purchases DESC
LIMIT 10;

-- 7. Анализ эффективности поставщиков
SELECT 
    supplier_name,
    country,
    total_revenue,
    products_count,
    avg_product_price,
    round(total_revenue / products_count, 2) as revenue_per_product
FROM supplier_sales_mart
ORDER BY revenue_per_product DESC;

-- 8. Категории товаров по популярности
SELECT 
    category,
    count(*) as products_count,
    sum(total_quantity) as total_sold,
    sum(total_revenue) as total_revenue,
    round(avg(rating), 2) as avg_rating
FROM product_sales_mart
GROUP BY category
ORDER BY total_revenue DESC;

-- 9. География продаж по странам
SELECT 
    country,
    count(*) as stores_count,
    sum(total_revenue) as total_revenue,
    sum(orders_count) as total_orders,
    round(avg(avg_check), 2) as avg_check
FROM store_sales_mart
GROUP BY country
ORDER BY total_revenue DESC;

-- 10. Анализ количества заказов по месяцам
SELECT 
    year,
    month_name,
    sum(orders_count) as monthly_orders,
    round(sum(orders_count) / 30.0, 1) as avg_daily_orders,
    round(sum(total_revenue) / sum(orders_count), 2) as avg_order_value
FROM time_sales_mart
GROUP BY year, month_name
ORDER BY year; 
