from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, dayofmonth, to_date,
    dayofweek, date_format, weekofyear, quarter
)

def create_spark_session():
    return (SparkSession.builder
            .appName("CreateStarSchema")
            .config("spark.jars", "/opt/spark-apps/jars/postgresql-42.6.0.jar")
            .getOrCreate())

def read_source_data(spark):
    return (spark.read
            .format("jdbc")
            .option("url", "jdbc:postgresql://postgres:5432/bigdata")
            .option("dbtable", "mock_data")
            .option("user", "postgres")
            .option("password", "pswd")
            .option("driver", "org.postgresql.Driver")
            .load())

def create_d_customers(df):
    return (df.select(
        col("customer_first_name").alias("first_name"),
        col("customer_last_name").alias("last_name"),
        col("customer_age").alias("age"),
        col("customer_email").alias("email"),
        col("customer_country").alias("country"),
        col("customer_postal_code").alias("postal_code"),
        col("customer_pet_type").alias("pet_type"),
        col("customer_pet_name").alias("pet_name"),
        col("customer_pet_breed").alias("pet_breed")
    ).dropDuplicates())

def create_d_sellers(df):
    return (df.select(
        col("seller_first_name").alias("first_name"),
        col("seller_last_name").alias("last_name"),
        col("seller_email").alias("email"),
        col("seller_country").alias("country"),
        col("seller_postal_code").alias("postal_code")
    ).dropDuplicates())

def create_d_products(df):
    return (df.select(
        col("product_name").alias("name"),
        col("product_category").alias("category"),
        col("product_price").alias("price"),
        col("product_weight").alias("weight"),
        col("product_color").alias("color"),
        col("product_size").alias("size"),
        col("product_brand").alias("brand"),
        col("product_material").alias("material"),
        col("product_description").alias("description"),
        col("product_rating").alias("rating"),
        col("product_reviews").alias("reviews"),
        col("product_release_date").alias("release_date"),
        col("product_expiry_date").alias("expiry_date")
    ).dropDuplicates())

def create_d_stores(df):
    return (df.select(
        col("store_name").alias("name"),
        col("store_location").alias("location"),
        col("store_city").alias("city"),
        col("store_state").alias("state"),
        col("store_country").alias("country"),
        col("store_phone").alias("phone"),
        col("store_email").alias("email")
    ).dropDuplicates())

def create_d_suppliers(df):
    return (df.select(
        col("supplier_name").alias("name"),
        col("supplier_contact").alias("contact"),
        col("supplier_email").alias("email"),
        col("supplier_phone").alias("phone"),
        col("supplier_address").alias("address"),
        col("supplier_city").alias("city"),
        col("supplier_country").alias("country")
    ).dropDuplicates())

def create_d_dates(df):
    # Сначала выделяем уникальные даты
    dates_df = df.select(
        to_date(col("sale_date"), "M/d/yyyy").alias("date")
    ).dropDuplicates()
    
    # Добавляем все временные измерения
    return dates_df.withColumn("year", year("date")) \
                  .withColumn("month", month("date")) \
                  .withColumn("day", dayofmonth("date")) \
                  .withColumn("weekday", dayofweek("date")) \
                  .withColumn("quarter", quarter("date")) \
                  .withColumn("month_name", date_format("date", "MMMM")) \
                  .withColumn("week_of_year", weekofyear("date"))

def read_dimension_table(spark, table_name):
    return (spark.read
            .format("jdbc")
            .option("url", "jdbc:postgresql://postgres:5432/bigdata")
            .option("dbtable", table_name)
            .option("user", "postgres")
            .option("password", "pswd")
            .option("driver", "org.postgresql.Driver")
            .load())

def create_f_sales(raw_df, customers_df, sellers_df, products_df, stores_df, suppliers_df, dates_df):
    # Подготавливаем дату для джойна
    df_with_date = raw_df.withColumn("sale_date", to_date(col("sale_date"), "M/d/yyyy"))
    
    # Джойним все измерения для получения их ID
    return (df_with_date
        .join(
            customers_df.alias("cust"),
            (df_with_date.customer_email == customers_df.email),
            "left"
        )
        .join(
            sellers_df.alias("sell"),
            (df_with_date.seller_email == sellers_df.email),
            "left"
        )
        .join(
            products_df.alias("prod"),
            (df_with_date.product_name == products_df.name) &
            (df_with_date.product_category == products_df.category) &
            (df_with_date.product_price == products_df.price) &
            (df_with_date.product_brand == products_df.brand) &
            (df_with_date.product_size == products_df.size) &
            (df_with_date.product_material == products_df.material),
            "left"
        )
        .join(
            stores_df.alias("store"),
            (df_with_date.store_email == stores_df.email),
            "left"
        )
        .join(
            suppliers_df.alias("sup"),
            (df_with_date.supplier_email == suppliers_df.email),
            "left"
        )
        .join(
            dates_df.alias("dt"),
            df_with_date.sale_date == dates_df.date,
            "left"
        )
        .select(
            col("cust.customer_id").alias("customer_id"),
            col("sell.seller_id").alias("seller_id"),
            col("prod.product_id").alias("product_id"),
            col("store.store_id").alias("store_id"),
            col("sup.supplier_id").alias("supplier_id"),
            col("dt.date_id").alias("date_id"),
            col("sale_quantity").cast("int").alias("sale_quantity"),
            col("sale_total_price").cast("decimal(10,2)").alias("sale_total_price")
        ))

def write_to_postgres(df, table_name):
    (df.write
        .format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/bigdata")
        .option("dbtable", table_name)
        .option("user", "postgres")
        .option("password", "pswd")
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save())

def main():
    spark = create_spark_session()
    
    # Читаем исходные данные
    source_df = read_source_data(spark)
    
    # Создаем таблицы измерений
    d_customers_df = create_d_customers(source_df)
    d_sellers_df = create_d_sellers(source_df)
    d_products_df = create_d_products(source_df)
    d_stores_df = create_d_stores(source_df)
    d_suppliers_df = create_d_suppliers(source_df)
    d_dates_df = create_d_dates(source_df)
    
    # Сохраняем таблицы измерений
    write_to_postgres(d_customers_df, "d_customers")
    write_to_postgres(d_sellers_df, "d_sellers")
    write_to_postgres(d_products_df, "d_products")
    write_to_postgres(d_stores_df, "d_stores")
    write_to_postgres(d_suppliers_df, "d_suppliers")
    write_to_postgres(d_dates_df, "d_dates")
    
    # Читаем таблицы измерений с ID из базы данных
    d_customers_with_ids = read_dimension_table(spark, "d_customers")
    d_sellers_with_ids = read_dimension_table(spark, "d_sellers")
    d_products_with_ids = read_dimension_table(spark, "d_products")
    d_stores_with_ids = read_dimension_table(spark, "d_stores")
    d_suppliers_with_ids = read_dimension_table(spark, "d_suppliers")
    d_dates_with_ids = read_dimension_table(spark, "d_dates")
    
    # Создаем и сохраняем таблицу фактов, используя DataFrame'ы с ID
    f_sales_df = create_f_sales(
        source_df,
        d_customers_with_ids,
        d_sellers_with_ids,
        d_products_with_ids,
        d_stores_with_ids,
        d_suppliers_with_ids,
        d_dates_with_ids
    )
    write_to_postgres(f_sales_df, "f_sales")
    
    spark.stop()

if __name__ == "__main__":
    main()
