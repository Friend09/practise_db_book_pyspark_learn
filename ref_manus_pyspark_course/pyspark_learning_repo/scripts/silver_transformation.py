import os
import sys

# Add the project root to the Python path to import custom modules
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if project_root not in sys.path:
    sys.path.append(project_root)

from conf.spark_session_config import get_spark_session
from pyspark.sql import functions as F

def transform_sales_data(spark, input_path="data/bronze/sales", output_path="data/silver/sales"):
    """
    Reads sales data from Bronze layer, transforms it, and writes to Silver layer.
    """
    print(f"Reading data from Bronze layer at {input_path}...")

    # Check if input path exists
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input path not found: {input_path}")

    df = spark.read.parquet(input_path)

    print("Original data schema:")
    df.printSchema()

    print("Applying transformations...")

    # Add calculated columns
    transformed_df = df.withColumn("total_price", F.col("quantity") * F.col("price")) \
                       .withColumn("processing_timestamp", F.current_timestamp()) \
                       .withColumn("transaction_date", F.to_date(F.col("transaction_date"), "yyyy-MM-dd"))

    print("Transformed data schema:")
    transformed_df.printSchema()

    print(f"Number of records: {transformed_df.count()}")

    print(f"Writing transformed data to Silver layer at {output_path}...")

    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Write as Parquet instead of Delta to avoid JAR issues
    transformed_df.write.mode("overwrite").parquet(output_path)

    print("Silver layer transformation complete.")

    # Show sample data
    print("Sample transformed data:")
    transformed_df.show(5)

    # Show some statistics
    print("Data statistics:")
    transformed_df.select(
        F.count("*").alias("total_records"),
        F.sum("total_price").alias("total_revenue"),
        F.avg("total_price").alias("avg_order_value"),
        F.countDistinct("customer_name").alias("unique_customers"),
        F.countDistinct("product_name").alias("unique_products")
    ).show()

if __name__ == "__main__":
    spark = get_spark_session(app_name="SilverLayerTransformation")
    try:
        transform_sales_data(spark)
    finally:
        spark.stop()
        print("SparkSession stopped.")
