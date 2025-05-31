-- Создаем базу данных для витрин
CREATE DATABASE IF NOT EXISTS sales_marts;

-- Используем созданную базу данных
USE sales_marts;

-- Удаляем существующие таблицы
DROP TABLE IF EXISTS product_sales_mart;
DROP TABLE IF EXISTS customer_sales_mart;
DROP TABLE IF EXISTS time_sales_mart;
DROP TABLE IF EXISTS store_sales_mart;
DROP TABLE IF EXISTS supplier_sales_mart;
DROP TABLE IF EXISTS product_quality_mart;

-- 1. Витрина продаж по продуктам
CREATE TABLE IF NOT EXISTS product_sales_mart
(
    -- Основные метрики для анализа продаж
    category String,
    product_name String,
    total_quantity UInt32,
    total_revenue Decimal(15,2),
    avg_price Decimal(15,2),
    -- Метрики для анализа популярности
    rating Decimal(5,2),
    reviews_count UInt32
)
ENGINE = MergeTree()
ORDER BY (category, product_name);

-- 2. Витрина продаж по клиентам
CREATE TABLE IF NOT EXISTS customer_sales_mart
(
    -- Идентификация клиента
    first_name String,
    last_name String,
    country String,
    -- Метрики покупок
    total_purchases Decimal(15,2),
    orders_count UInt32,
    avg_check Decimal(15,2)
)
ENGINE = MergeTree()
ORDER BY (country, last_name, first_name);

-- 3. Витрина продаж по времени
CREATE TABLE IF NOT EXISTS time_sales_mart
(
    -- Временные измерения
    date Date,
    year UInt16,
    month UInt8,
    month_name String,
    -- Метрики продаж
    total_revenue Decimal(15,2),
    orders_count UInt32,
    avg_order_size Decimal(15,2),
    -- Для сравнения с предыдущими периодами
    prev_month_revenue Decimal(15,2),
    revenue_growth_percent Decimal(5,2)
)
ENGINE = MergeTree()
ORDER BY (year, month, date);

-- 4. Витрина продаж по магазинам
CREATE TABLE IF NOT EXISTS store_sales_mart
(
    -- Информация о магазине
    store_name String,
    city String,
    country String,
    -- Метрики продаж
    total_revenue Decimal(15,2),
    orders_count UInt32,
    avg_check Decimal(15,2)
)
ENGINE = MergeTree()
ORDER BY (country, city, store_name);

-- 5. Витрина продаж по поставщикам
CREATE TABLE IF NOT EXISTS supplier_sales_mart
(
    -- Информация о поставщике
    supplier_name String,
    country String,
    -- Метрики продаж и товаров
    total_revenue Decimal(15,2),
    products_count UInt32,
    avg_product_price Decimal(15,2)
)
ENGINE = MergeTree()
ORDER BY (country, supplier_name);

-- 6. Витрина качества продукции
CREATE TABLE IF NOT EXISTS product_quality_mart
(
    -- Информация о продукте
    product_name String,
    category String,
    -- Метрики качества
    rating Decimal(5,2),
    reviews_count UInt32,
    -- Метрики продаж для корреляции
    sales_count UInt32,
    total_revenue Decimal(15,2)
)
ENGINE = MergeTree()
ORDER BY (category, product_name);
