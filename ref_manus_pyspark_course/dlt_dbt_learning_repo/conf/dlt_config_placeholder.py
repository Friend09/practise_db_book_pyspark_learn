# DLT configurations are primarily defined within the DLT pipeline Python scripts themselves.
# This file serves as a placeholder for any shared DLT-related configurations or utility functions
# that might be used across multiple DLT pipelines.

# Example: Define a schema for raw data if not inferring
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# raw_sales_schema = StructType([
#     StructField("transaction_id", IntegerType(), True),
#     StructField("product_name", StringType(), True),
#     StructField("customer_name", StringType(), True),
#     StructField("city", StringType(), True),
#     StructField("quantity", IntegerType(), True),
#     StructField("price", DoubleType(), True),
#     StructField("transaction_date", StringType(), True)
# ])


