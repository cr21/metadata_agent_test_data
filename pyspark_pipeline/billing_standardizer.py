from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, lit

spark = SparkSession.builder.appName("BillingStandardizer").getOrCreate()

# Source: Raw Billing Data
input_path = "gs://raw-zone/billing/customer_invoices_raw.csv"
df = spark.read.csv(input_path, header=True)

# Transformations: Testing internal column-level mapping
cleaned_df = df.select(
    col("invoice_id").alias("billing_id"),
    col("cust_name"),
    trim(col("currency")).alias("currency_code"),
    # Derived logic: if amount is negative, set to 0
    when(col("amount") > 0, col("amount")).otherwise(lit(0)).alias("billable_amount"),
    col("invoice_date").cast("date")
)

# Sink: Standardized Billing
output_path = "gs://curated-zone/billing/standardized_invoices/"
cleaned_df.write.mode("overwrite").parquet(output_path)