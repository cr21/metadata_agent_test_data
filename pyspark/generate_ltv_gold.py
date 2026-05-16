from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count

spark = SparkSession.builder.appName("Gold_Customer_LTV").getOrCreate()

# Source: Reading from the Silver table produced in the previous script
orders = spark.read.table("silver_db.orders_cleaned")
customers = spark.read.table("silver_db.customers_cleaned")

# Business Logic: Join and Aggregation to create Lifetime Value (LTV)
gold_ltv = orders.join(customers, "customer_id") \
    .groupBy("customer_id", "customer_name") \
    .agg(
        sum("order_val").alias("total_lifetime_value"),
        count("order_id").alias("order_count")
    )

# Sink: Final analytics-ready product
gold_ltv.write.mode("overwrite").saveAsTable("gold_db.marketing_ltv_summary")