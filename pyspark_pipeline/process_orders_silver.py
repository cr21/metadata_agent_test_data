from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Silver_Orders").getOrCreate()

# Sources: Bronzes/Raw files
orders_raw = spark.read.parquet("gs://bronze/orders_raw/")

# Business Logic: Filter completed orders and calculate line-item value
orders_silver = orders_raw.filter(col("order_status") == 'COMPLETED') \
    .withColumn("order_val", col("price") * col("quantity"))

# Sink: Writing to a Hive/BigQuery Metastore table
orders_silver.write.mode("overwrite").saveAsTable("silver_db.orders_cleaned")