from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, when, monotonically_increasing_id, to_date, regexp_replace,
    year, month, dayofmonth, quarter, dayofweek, weekofyear, date_format, lit 
)
from pyspark.sql.types import IntegerType, DecimalType, DateType, StringType

def main():
    spark = SparkSession.builder \
        .appName("PostgresRawToStarSchema") \
        .config("spark.driver.extraClassPath", "/home/jovyan/drivers/postgresql.jar") \
        .config("spark.executor.extraClassPath", "/home/jovyan/drivers/postgresql.jar") \
        .getOrCreate()

    db_properties = {
        "user": "user",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }
    postgres_url = "jdbc:postgresql://postgres:5432/bigdata_store"

    df_raw = spark.read.jdbc(url=postgres_url, table="mock_data_raw", properties=db_properties)

    def clean_col(c):
        return trim(col(c))

    def to_int(c):
        return clean_col(c).cast(IntegerType())

    def to_decimal(c, precision=10, scale=2):
        return regexp_replace(clean_col(c), r"[^0-9.]", "").cast(DecimalType(precision, scale))



    df_customers = df_raw.select(
        clean_col("sale_customer_id").alias("customer_id_source"),
        clean_col("customer_first_name").alias("first_name"),
        clean_col("customer_last_name").alias("last_name"),
        to_int("customer_age").alias("age"),
        clean_col("customer_email").alias("email"),
        clean_col("customer_country").alias("country"),
        clean_col("customer_postal_code").alias("postal_code"),
        clean_col("customer_pet_type").alias("pet_type"),
        clean_col("customer_pet_name").alias("pet_name"),
        clean_col("customer_pet_breed").alias("pet_breed")
    ).filter(col("email").isNotNull()).distinct() \
     .withColumn("customer_pk", monotonically_increasing_id())

    df_sellers = df_raw.select(
        to_int("sale_seller_id").alias("seller_id_source"),
        clean_col("seller_first_name").alias("first_name"),
        clean_col("seller_last_name").alias("last_name"),
        clean_col("seller_email").alias("email"),
        clean_col("seller_country").alias("country"),
        clean_col("seller_postal_code").alias("postal_code")
    ).filter(col("email").isNotNull()).distinct() \
     .withColumn("seller_pk", monotonically_increasing_id())

    df_suppliers = df_raw.select(
        clean_col("supplier_name").alias("name"),
        clean_col("supplier_contact").alias("contact_person"),
        clean_col("supplier_email").alias("email"),
        clean_col("supplier_phone").alias("phone"),
        clean_col("supplier_address").alias("address"),
        clean_col("supplier_city").alias("city"),
        clean_col("supplier_country").alias("country")
    ).filter(col("name").isNotNull()).distinct() \
     .withColumn("supplier_pk", monotonically_increasing_id())

    df_stores = df_raw.select(
        clean_col("store_name").alias("name"),
        clean_col("store_location").alias("location_details"),
        clean_col("store_city").alias("city"),
        clean_col("store_state").alias("state_code"),
        clean_col("store_country").alias("country"),
        clean_col("store_phone").alias("phone"),
        clean_col("store_email").alias("email")
    ).filter(col("name").isNotNull() & col("email").isNotNull()).distinct() \
     .withColumn("store_pk", monotonically_increasing_id())

    df_product_categories = df_raw.select(
        clean_col("product_category").alias("category_name")
    ).filter(col("category_name").isNotNull()).distinct() \
     .withColumn("category_pk", monotonically_increasing_id())

    df_product_brands = df_raw.select(
        clean_col("product_brand").alias("brand_name")
    ).filter(col("brand_name").isNotNull()).distinct() \
     .withColumn("brand_pk", monotonically_increasing_id())

    df_product_pet_categories = df_raw.select(
        clean_col("pet_category").alias("pet_category_name")
    ).filter(col("pet_category_name").isNotNull()).distinct() \
     .withColumn("pet_category_pk", monotonically_increasing_id())

    df_products_intermediate = df_raw.select(
        to_int("sale_product_id").alias("product_id_source"),
        clean_col("product_name").alias("name"),
        clean_col("product_category"),
        to_decimal("product_price").alias("price"),
        to_int("product_quantity").alias("inventory_quantity"),
        to_decimal("product_weight").alias("weight_grams"),
        clean_col("product_color").alias("color"),
        clean_col("product_size").alias("size_description"),
        clean_col("product_brand"),
        clean_col("product_material").alias("material"),
        clean_col("product_description").alias("description"),
        to_decimal("product_rating", 3, 1).alias("rating"),
        to_int("product_reviews").alias("reviews_count"),
        to_date(clean_col("product_release_date"), "MM/dd/yyyy").alias("release_date"),
        to_date(clean_col("product_expiry_date"), "MM/dd/yyyy").alias("expiry_date"),
        clean_col("supplier_name"), 
        clean_col("pet_category")
    ).filter(col("name").isNotNull() & col("product_brand").isNotNull())

    df_products = df_products_intermediate \
        .join(df_product_categories, df_products_intermediate.product_category == df_product_categories.category_name, "left") \
        .join(df_product_brands, df_products_intermediate.product_brand == df_product_brands.brand_name, "left") \
        .join(df_suppliers, df_products_intermediate.supplier_name == df_suppliers.name, "left") \
        .join(df_product_pet_categories, df_products_intermediate.pet_category == df_product_pet_categories.pet_category_name, "left") \
        .select(
            "product_id_source", "name", col("category_pk").alias("category_fk"), "price", "inventory_quantity",
            "weight_grams", "color", "size_description", col("brand_pk").alias("brand_fk"), "material",
            "description", "rating", "reviews_count", "release_date", "expiry_date",
            col("supplier_pk").alias("supplier_fk"), col("pet_category_pk").alias("pet_category_fk")
        ).distinct() \
        .withColumn("product_pk", monotonically_increasing_id())

    
    dim_tables_map = {
        "DimCustomers": df_customers,
        "DimSellers": df_sellers,
        "DimSuppliers": df_suppliers,
        "DimStores": df_stores,
        "DimProductCategories": df_product_categories,
        "DimProductBrands": df_product_brands,
        "DimProductPetCategories": df_product_pet_categories,
        "DimProducts": df_products
    }

    for table_name, df_dim in dim_tables_map.items():
        print(f"Writing {table_name}...")
        df_dim.write.jdbc(url=postgres_url, table=table_name, mode="overwrite", properties=db_properties)
        print(f"{table_name} written successfully.")


    df_fact_sales_intermediate = df_raw.select(
        to_date(clean_col("sale_date"), "MM/dd/yyyy").alias("sale_date_parsed"),
        clean_col("customer_email"), 
        clean_col("seller_email"), 
        clean_col("product_name"),
        clean_col("product_brand"), 
        clean_col("store_name"), 
        clean_col("store_email"), 
        to_int("sale_quantity").alias("quantity_sold"),
        to_decimal("sale_total_price").alias("total_price"),
        to_int("id").alias("source_id"),
        col("source_file_row_id").alias("source_file_row_id_fk")
    )

    df_dim_date = df_fact_sales_intermediate.select(col("sale_date_parsed").alias("date_actual")).distinct() \
        .filter(col("date_actual").isNotNull()) \
        .withColumn("date_pk_str", date_format(col("date_actual"), "yyyyMMdd")) \
        .withColumn("date_pk", col("date_pk_str").cast(IntegerType())) \
        .withColumn("year", year(col("date_actual"))) \
        .withColumn("month", month(col("date_actual"))) \
        .withColumn("day", dayofmonth(col("date_actual"))) \
        .withColumn("quarter", quarter(col("date_actual"))) \
        .withColumn("day_of_week", dayofweek(col("date_actual"))) \
        .withColumn("week_of_year", weekofyear(col("date_actual"))) \
        .withColumn("is_weekend", when(dayofweek(col("date_actual")).isin([1, 7]), True).otherwise(False)) \
        .select("date_pk", "date_actual", "year", "month", "day", "quarter", "day_of_week", "week_of_year", "is_weekend")
    
    print("Writing DimDate...")
    df_dim_date.write.jdbc(url=postgres_url, table="DimDate", mode="overwrite", properties=db_properties)
    print("DimDate written successfully.")


    df_fact_sales = df_fact_sales_intermediate \
        .join(df_dim_date, df_fact_sales_intermediate.sale_date_parsed == df_dim_date.date_actual, "left") \
        .join(df_customers, df_fact_sales_intermediate.customer_email == df_customers.email, "left") \
        .join(df_sellers, df_fact_sales_intermediate.seller_email == df_sellers.email, "left") \
        .join(df_stores, (df_fact_sales_intermediate.store_name == df_stores.name) & \
                         (df_fact_sales_intermediate.store_email == df_stores.email), "left") \
        .join(df_products.alias("p"), 
              (df_fact_sales_intermediate.product_name == col("p.name")) & \
              (df_fact_sales_intermediate.product_brand == df_product_brands.filter(df_product_brands.brand_pk == col("p.brand_fk")).select("brand_name").first()[0] if df_product_brands.filter(df_product_brands.brand_pk == col("p.brand_fk")).count() > 0 else lit(None).cast(StringType()) ), "left") \
        .select(
            col("date_pk").alias("date_fk"),
            col("customer_pk").alias("customer_fk"),
            col("seller_pk").alias("seller_fk"),
            col("p.product_pk").alias("product_fk"),
            col("store_pk").alias("store_fk"),
            "quantity_sold",
            "total_price",
            "source_id",
            "source_file_row_id_fk"
        ).withColumn("sales_pk", monotonically_increasing_id())

    
    df_fact_sales_intermediate_with_brand_pk = df_fact_sales_intermediate \
        .join(df_product_brands, df_fact_sales_intermediate.product_brand == df_product_brands.brand_name, "left")

    df_fact_sales = df_fact_sales_intermediate_with_brand_pk \
        .join(df_dim_date, df_fact_sales_intermediate_with_brand_pk.sale_date_parsed == df_dim_date.date_actual, "left") \
        .join(df_customers, df_fact_sales_intermediate_with_brand_pk.customer_email == df_customers.email, "left") \
        .join(df_sellers, df_fact_sales_intermediate_with_brand_pk.seller_email == df_sellers.email, "left") \
        .join(df_stores, (df_fact_sales_intermediate_with_brand_pk.store_name == df_stores.name) & \
                         (df_fact_sales_intermediate_with_brand_pk.store_email == df_stores.email), "left") \
        .join(df_products.alias("p"),
              (df_fact_sales_intermediate_with_brand_pk.product_name == col("p.name")) & \
              (df_fact_sales_intermediate_with_brand_pk.brand_pk == col("p.brand_fk")), "left") \
        .select(
            col("date_pk").alias("date_fk"),
            col("customer_pk").alias("customer_fk"),
            col("seller_pk").alias("seller_fk"),
            col("p.product_pk").alias("product_fk"),
            col("store_pk").alias("store_fk"),
            "quantity_sold",
            "total_price",
            "source_id",
            "source_file_row_id_fk"
        ).withColumn("sales_pk", monotonically_increasing_id())


    print("Writing FactSales...")
    df_fact_sales.write.jdbc(url=postgres_url, table="FactSales", mode="overwrite", properties=db_properties)
    print("FactSales written successfully.")

    spark.stop()

if __name__ == "__main__":
    main()