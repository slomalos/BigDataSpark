from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, sum, avg, count, desc, rank, month, year, date_format, corr, asc

def main():
    spark = SparkSession.builder \
        .appName("StarSchemaToReports") \
        .config("spark.driver.extraClassPath", "/home/jovyan/drivers/postgresql.jar,/home/jovyan/drivers/clickhouse.jar") \
        .config("spark.executor.extraClassPath", "/home/jovyan/drivers/postgresql.jar,/home/jovyan/drivers/clickhouse.jar") \
        .getOrCreate()

    pg_properties = {
        "user": "user",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }
    postgres_url = "jdbc:postgresql://postgres:5432/bigdata_store"

    clickhouse_url = "jdbc:clickhouse://clickhouse:8123/default"

    df_fact_sales = spark.read.jdbc(url=postgres_url, table="FactSales", properties=pg_properties)
    df_dim_products = spark.read.jdbc(url=postgres_url, table="DimProducts", properties=pg_properties)
    df_dim_customers = spark.read.jdbc(url=postgres_url, table="DimCustomers", properties=pg_properties)
    df_dim_date = spark.read.jdbc(url=postgres_url, table="DimDate", properties=pg_properties)
    df_dim_stores = spark.read.jdbc(url=postgres_url, table="DimStores", properties=pg_properties)
    df_dim_suppliers = spark.read.jdbc(url=postgres_url, table="DimSuppliers", properties=pg_properties)
    df_dim_product_categories = spark.read.jdbc(url=postgres_url, table="DimProductCategories", properties=pg_properties)

    def save_report_to_clickhouse(df_report, ch_table_name): 
        print(f"Writing report to ClickHouse table: {ch_table_name}")
        df_report.write.format("jdbc") \
            .option("url", clickhouse_url) \
            .option("dbtable", ch_table_name) \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .option("createTableOptions", "ENGINE = MergeTree() ORDER BY tuple()") \
            .mode("overwrite") \
            .save()
        print(f"Report {ch_table_name} written to ClickHouse.")

    report_top_10_products = df_fact_sales.join(df_dim_products, df_fact_sales.product_fk == df_dim_products.product_pk) \
        .groupBy(df_dim_products.name.alias("product_name")) \
        .agg(sum("total_price").alias("total_revenue"), sum("quantity_sold").alias("total_quantity_sold")) \
        .orderBy(desc("total_revenue")) \
        .limit(10)
    save_report_to_clickhouse(report_top_10_products, "report_top_10_selling_products")

    report_revenue_by_category = df_fact_sales \
        .join(df_dim_products, df_fact_sales.product_fk == df_dim_products.product_pk) \
        .join(df_dim_product_categories, df_dim_products.category_fk == df_dim_product_categories.category_pk) \
        .groupBy(df_dim_product_categories.category_name) \
        .agg(sum("total_price").alias("total_revenue")) \
        .orderBy(desc("total_revenue"))
    save_report_to_clickhouse(report_revenue_by_category, "report_revenue_by_category")

    report_product_ratings_reviews = df_dim_products \
        .select(
            col("name").alias("product_name"),
            col("rating").alias("average_rating"),
            col("reviews_count")
        ).orderBy(desc("average_rating"), desc("reviews_count"))
    save_report_to_clickhouse(report_product_ratings_reviews, "report_product_ratings_reviews")

    report_top_10_customers = df_fact_sales.join(df_dim_customers, df_fact_sales.customer_fk == df_dim_customers.customer_pk) \
        .groupBy(df_dim_customers.email.alias("customer_email"), df_dim_customers.first_name, df_dim_customers.last_name) \
        .agg(sum("total_price").alias("total_purchase_amount")) \
        .orderBy(desc("total_purchase_amount")) \
        .limit(10)
    save_report_to_clickhouse(report_top_10_customers, "report_top_10_customers_by_purchase")

    report_customers_by_country = df_dim_customers \
        .groupBy("country") \
        .agg(count("*").alias("number_of_customers")) \
        .orderBy(desc("number_of_customers"))
    save_report_to_clickhouse(report_customers_by_country, "report_customers_by_country")

    report_avg_check_per_customer = df_fact_sales.join(df_dim_customers, df_fact_sales.customer_fk == df_dim_customers.customer_pk) \
        .groupBy(df_dim_customers.email.alias("customer_email")) \
        .agg(avg("total_price").alias("average_order_value"), sum("total_price").alias("total_spent"), count("*").alias("order_count") ) \
        .orderBy(desc("total_spent"))
    save_report_to_clickhouse(report_avg_check_per_customer, "report_avg_check_per_customer")


    report_sales_trends = df_fact_sales.join(df_dim_date, df_fact_sales.date_fk == df_dim_date.date_pk) \
        .groupBy(df_dim_date.year, df_dim_date.month) \
        .agg(sum("total_price").alias("monthly_revenue"), sum("quantity_sold").alias("monthly_quantity")) \
        .orderBy("year", "month")
    save_report_to_clickhouse(report_sales_trends, "report_monthly_sales_trends")
    
    report_avg_order_size_monthly = df_fact_sales.join(df_dim_date, df_fact_sales.date_fk == df_dim_date.date_pk) \
        .groupBy(df_dim_date.year, df_dim_date.month) \
        .agg(avg("total_price").alias("average_order_size")) \
        .orderBy("year", "month")
    save_report_to_clickhouse(report_avg_order_size_monthly, "report_avg_order_size_monthly")


    report_top_5_stores = df_fact_sales.join(df_dim_stores, df_fact_sales.store_fk == df_dim_stores.store_pk) \
        .groupBy(df_dim_stores.name.alias("store_name"), df_dim_stores.city, df_dim_stores.country) \
        .agg(sum("total_price").alias("total_revenue")) \
        .orderBy(desc("total_revenue")) \
        .limit(5)
    save_report_to_clickhouse(report_top_5_stores, "report_top_5_stores_by_revenue")

    report_sales_by_store_location = df_fact_sales.join(df_dim_stores, df_fact_sales.store_fk == df_dim_stores.store_pk) \
        .groupBy(df_dim_stores.country, df_dim_stores.city) \
        .agg(sum("total_price").alias("total_revenue"), count("*").alias("number_of_sales")) \
        .orderBy(desc("total_revenue"))
    save_report_to_clickhouse(report_sales_by_store_location, "report_sales_by_store_location")

    report_avg_check_per_store = df_fact_sales.join(df_dim_stores, df_fact_sales.store_fk == df_dim_stores.store_pk) \
        .groupBy(df_dim_stores.name.alias("store_name"), df_dim_stores.city, df_dim_stores.country) \
        .agg(avg("total_price").alias("average_order_value")) \
        .orderBy(desc("average_order_value"))
    save_report_to_clickhouse(report_avg_check_per_store, "report_avg_check_per_store")


    report_top_5_suppliers = df_fact_sales \
        .join(df_dim_products, df_fact_sales.product_fk == df_dim_products.product_pk) \
        .join(df_dim_suppliers, df_dim_products.supplier_fk == df_dim_suppliers.supplier_pk) \
        .groupBy(df_dim_suppliers.name.alias("supplier_name")) \
        .agg(sum("total_price").alias("total_revenue_from_supplied_products")) \
        .orderBy(desc("total_revenue_from_supplied_products")) \
        .limit(5)
    save_report_to_clickhouse(report_top_5_suppliers, "report_top_5_suppliers_by_revenue")
    
    report_avg_product_price_by_supplier = df_dim_products \
        .join(df_dim_suppliers, df_dim_products.supplier_fk == df_dim_suppliers.supplier_pk) \
        .groupBy(df_dim_suppliers.name.alias("supplier_name")) \
        .agg(avg("price").alias("average_product_price")) \
        .orderBy(desc("average_product_price"))
    save_report_to_clickhouse(report_avg_product_price_by_supplier, "report_avg_product_price_by_supplier")

    report_sales_by_supplier_country = df_fact_sales \
        .join(df_dim_products, df_fact_sales.product_fk == df_dim_products.product_pk) \
        .join(df_dim_suppliers, df_dim_products.supplier_fk == df_dim_suppliers.supplier_pk) \
        .groupBy(df_dim_suppliers.country.alias("supplier_country")) \
        .agg(sum("total_price").alias("total_revenue")) \
        .orderBy(desc("total_revenue"))
    save_report_to_clickhouse(report_sales_by_supplier_country, "report_sales_by_supplier_country")

    window_spec_rating_desc = Window.orderBy(desc("rating"))
    window_spec_rating_asc = Window.orderBy(asc("rating"))
    
    df_product_ratings_ranked = df_dim_products \
        .filter(col("rating").isNotNull()) \
        .withColumn("rank_desc", rank().over(window_spec_rating_desc)) \
        .withColumn("rank_asc", rank().over(window_spec_rating_asc))
        
    report_highest_rated_products = df_product_ratings_ranked \
        .filter(col("rank_desc") <= 5) \
        .select("name", "rating").orderBy(desc("rating"))
    save_report_to_clickhouse(report_highest_rated_products, "report_highest_rated_products")
    
    report_lowest_rated_products = df_product_ratings_ranked \
        .filter(col("rank_asc") <= 5) \
        .select("name", "rating").orderBy(asc("rating"))
    save_report_to_clickhouse(report_lowest_rated_products, "report_lowest_rated_products")

    correlation_df = df_fact_sales \
        .join(df_dim_products, df_fact_sales.product_fk == df_dim_products.product_pk) \
        .filter(col("rating").isNotNull()) \
        .agg(corr("rating", "total_price").alias("rating_revenue_correlation"))
    save_report_to_clickhouse(correlation_df, "report_rating_revenue_correlation")

    report_most_reviewed_products = df_dim_products \
        .filter(col("reviews_count").isNotNull()) \
        .select("name", "reviews_count") \
        .orderBy(desc("reviews_count")) \
        .limit(10)
    save_report_to_clickhouse(report_most_reviewed_products, "report_most_reviewed_products")

    spark.stop()

if __name__ == "__main__":
    main()