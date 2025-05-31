from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, avg, count, desc, first,
    lag, round, when, lit, countDistinct as distinct
)
from pyspark.sql.window import Window

def create_spark_session():
    return (SparkSession.builder
            .appName("CreateDataMarts")
            .config("spark.jars", "/opt/spark-apps/jars/postgresql-42.6.0.jar,/opt/spark-apps/jars/clickhouse-jdbc-0.4.6.jar")
            .getOrCreate())

def read_star_schema(spark):
    # Читаем таблицы из PostgreSQL
    common_options = {
        "url": "jdbc:postgresql://postgres:5432/bigdata",
        "user": "postgres",
        "password": "pswd",
        "driver": "org.postgresql.Driver"
    }
    
    # Читаем все таблицы
    f_sales = spark.read.format("jdbc").option("dbtable", "f_sales").options(**common_options).load()
    d_products = spark.read.format("jdbc").option("dbtable", "d_products").options(**common_options).load()
    d_customers = spark.read.format("jdbc").option("dbtable", "d_customers").options(**common_options).load()
    d_dates = spark.read.format("jdbc").option("dbtable", "d_dates").options(**common_options).load()
    d_stores = spark.read.format("jdbc").option("dbtable", "d_stores").options(**common_options).load()
    d_suppliers = spark.read.format("jdbc").option("dbtable", "d_suppliers").options(**common_options).load()
    
    return f_sales, d_products, d_customers, d_dates, d_stores, d_suppliers

def write_to_clickhouse(df, table_name):
    # Записываем DataFrame в Clickhouse
    (df.write
        .format("jdbc")
        .option("url", "jdbc:clickhouse://clickhouse:8123/sales_marts")
        .option("dbtable", table_name)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("user", "custom_user")
        .option("password", "pswd")
        .mode("append")
        .save())

def create_product_sales_mart(f_sales, d_products):
    return (f_sales
        .join(d_products, "product_id")
        .groupBy("category", "name")
        .agg(
            sum("sale_quantity").alias("total_quantity"),
            sum("sale_total_price").alias("total_revenue"),
            avg("price").alias("avg_price"),
            first("rating").alias("rating"),
            first("reviews").alias("reviews_count")
        )
        .select(
            col("category"),
            col("name").alias("product_name"),
            col("total_quantity"),
            col("total_revenue"),
            col("avg_price"),
            col("rating"),
            col("reviews_count")
        ))

def create_customer_sales_mart(f_sales, d_customers):
    return (f_sales
        .join(d_customers, "customer_id")
        .groupBy("first_name", "last_name", "country")
        .agg(
            sum("sale_total_price").alias("total_purchases"),
            count("*").alias("orders_count"),
            avg("sale_total_price").alias("avg_check")
        ))

def create_time_sales_mart(f_sales, d_dates):
    # Создаем оконную спецификацию для сравнения с предыдущим месяцем
    window_spec = Window.orderBy("year", "month")
    
    return (f_sales
        .join(d_dates, "date_id")
        .groupBy("date", "year", "month", "month_name")
        .agg(
            sum("sale_total_price").alias("total_revenue"),
            count("*").alias("orders_count"),
            avg("sale_total_price").alias("avg_order_size")
        )
        .withColumn("prev_month_revenue", 
            lag("total_revenue", 1).over(window_spec))
        .withColumn("revenue_growth_percent",
            round(((col("total_revenue") - col("prev_month_revenue")) / col("prev_month_revenue") * 100), 2))
        .na.fill(0, ["prev_month_revenue", "revenue_growth_percent"]))

def create_store_sales_mart(f_sales, d_stores):
    return (f_sales
        .join(d_stores, "store_id")
        .groupBy("name", "city", "country")
        .agg(
            sum("sale_total_price").alias("total_revenue"),
            count("*").alias("orders_count")
        )
        .withColumn("avg_check", round(col("total_revenue") / col("orders_count"), 2))
        .select(
            col("name").alias("store_name"),
            col("city"),
            col("country"),
            col("total_revenue"),
            col("orders_count"),
            col("avg_check")
        ))

def create_supplier_sales_mart(f_sales, d_products, d_suppliers):
    return (f_sales
        .join(d_products, "product_id")
        .join(d_suppliers, "supplier_id")
        .groupBy(d_suppliers.name, d_suppliers.country)
        .agg(
            sum("sale_total_price").alias("total_revenue"),
            distinct("product_id").alias("products_count"),
            avg("price").alias("avg_product_price")
        )
        .select(
            col("name").alias("supplier_name"),
            col("country"),
            col("total_revenue"),
            col("products_count"),
            col("avg_product_price")
        ))

def create_product_quality_mart(f_sales, d_products):
    return (f_sales
        .join(d_products, "product_id")
        .groupBy("name", "category", "rating", "reviews")
        .agg(
            count("*").alias("sales_count"),
            sum("sale_total_price").alias("total_revenue")
        )
        .select(
            col("name").alias("product_name"),
            col("category"),
            col("rating"),
            col("reviews").alias("reviews_count"),
            col("sales_count"),
            col("total_revenue")
        ))

def main():
    spark = create_spark_session()
    
    # Читаем данные из PostgreSQL
    f_sales, d_products, d_customers, d_dates, d_stores, d_suppliers = read_star_schema(spark)
    
    # Создаем и сохраняем витрины
    product_sales_df = create_product_sales_mart(f_sales, d_products)
    write_to_clickhouse(product_sales_df, "product_sales_mart")
    
    customer_sales_df = create_customer_sales_mart(f_sales, d_customers)
    write_to_clickhouse(customer_sales_df, "customer_sales_mart")
    
    time_sales_df = create_time_sales_mart(f_sales, d_dates)
    write_to_clickhouse(time_sales_df, "time_sales_mart")
    
    store_sales_df = create_store_sales_mart(f_sales, d_stores)
    write_to_clickhouse(store_sales_df, "store_sales_mart")
    
    supplier_sales_df = create_supplier_sales_mart(f_sales, d_products, d_suppliers)
    write_to_clickhouse(supplier_sales_df, "supplier_sales_mart")
    
    product_quality_df = create_product_quality_mart(f_sales, d_products)
    write_to_clickhouse(product_quality_df, "product_quality_mart")
    
    spark.stop()

if __name__ == "__main__":
    main()
