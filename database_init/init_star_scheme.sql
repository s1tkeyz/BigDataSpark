-- Удаляем существующие таблицы
DROP TABLE IF EXISTS f_sales CASCADE;
DROP TABLE IF EXISTS d_customers CASCADE;
DROP TABLE IF EXISTS d_sellers CASCADE;
DROP TABLE IF EXISTS d_products CASCADE;
DROP TABLE IF EXISTS d_stores CASCADE;
DROP TABLE IF EXISTS d_suppliers CASCADE;
DROP TABLE IF EXISTS d_dates CASCADE;

-- Создаем таблицы измерений
CREATE TABLE d_customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    age INT,
    email VARCHAR(50),
    country VARCHAR(50),
    postal_code VARCHAR(50),
    pet_type VARCHAR(50),
    pet_name VARCHAR(50),
    pet_breed VARCHAR(50)
);

CREATE TABLE d_sellers (
    seller_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(50),
    country VARCHAR(50),
    postal_code VARCHAR(50)
);

CREATE TABLE d_products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(50),
    category VARCHAR(50),
    price NUMERIC(10,2),
    weight REAL,
    color VARCHAR(50),
    size VARCHAR(50),
    brand VARCHAR(50),
    material VARCHAR(50),
    description TEXT,
    rating REAL,
    reviews INT,
    release_date VARCHAR(50),
    expiry_date VARCHAR(50)
);

CREATE TABLE d_stores (
    store_id SERIAL PRIMARY KEY,
    name VARCHAR(50),
    location VARCHAR(50),
    city VARCHAR(50),
    state VARCHAR(50),
    country VARCHAR(50),
    phone VARCHAR(50),
    email VARCHAR(50)
);

CREATE TABLE d_suppliers (
    supplier_id SERIAL PRIMARY KEY,
    name VARCHAR(50),
    contact VARCHAR(50),
    email VARCHAR(50),
    phone VARCHAR(50),
    address VARCHAR(50),
    city VARCHAR(50),
    country VARCHAR(50)
);

CREATE TABLE d_dates (
    date_id SERIAL PRIMARY KEY,
    date DATE,
    year INT,
    month INT,
    day INT,
    weekday INT,
    quarter INT,
    month_name VARCHAR(20),
    week_of_year INT
);

-- Создаем таблицу фактов с внешними ключами
CREATE TABLE f_sales (
    sale_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES d_customers(customer_id),
    seller_id INT REFERENCES d_sellers(seller_id),
    product_id INT REFERENCES d_products(product_id),
    store_id INT REFERENCES d_stores(store_id),
    supplier_id INT REFERENCES d_suppliers(supplier_id),
    date_id INT REFERENCES d_dates(date_id),
    sale_quantity INT,
    sale_total_price NUMERIC(10,2)
); 