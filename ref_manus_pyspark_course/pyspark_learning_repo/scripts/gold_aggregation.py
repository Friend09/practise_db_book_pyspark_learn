import os
import sys

# Add the project root to the Python path to import custom modules
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if project_root not in sys.path:
    sys.path.append(project_root)

from conf.spark_session_config import get_spark_session
from pyspark.sql import functions as F

def aggregate_sales_data(spark, input_path="data/silver/sales", output_path="data/gold/sales_summary"):
    """
    Reads sales data from Silver layer, aggregates it, and writes to Gold layer.
    """
    print(f"Reading data from Silver layer at {input_path}...")

    # Check if input path exists
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input path not found: {input_path}")

    df = spark.read.parquet(input_path)

    print("Silver layer data schema:")
    df.printSchema()

    print("Aggregating data...")

    # Create aggregated data for Gold layer
    aggregated_df = df.groupBy("product_name", "city") \
                        .agg(
                            F.sum("total_price").alias("total_revenue"),
                            F.count("transaction_id").alias("total_transactions"),
                            F.sum("quantity").alias("total_quantity_sold"),
                            F.avg("total_price").alias("avg_order_value"),
                            F.min("transaction_date").alias("first_sale_date"),
                            F.max("transaction_date").alias("last_sale_date")
                        ) \
                        .withColumn("revenue_per_transaction", F.col("total_revenue") / F.col("total_transactions")) \
                        .orderBy(F.col("total_revenue").desc())

    print("Aggregated data schema:")
    aggregated_df.printSchema()

    print(f"Number of product-city combinations: {aggregated_df.count()}")

    print(f"Writing aggregated data to Gold layer at {output_path}...")

    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Write as Parquet instead of Delta to avoid JAR issues
    aggregated_df.write.mode("overwrite").parquet(output_path)

    print("Gold layer aggregation complete.")

    # Show sample data
    print("Top 10 product-city combinations by revenue:")
    aggregated_df.show(10)

    # Show summary statistics
    print("Summary statistics:")
    aggregated_df.select(
        F.count("*").alias("total_combinations"),
        F.sum("total_revenue").alias("grand_total_revenue"),
        F.avg("total_revenue").alias("avg_revenue_per_combination"),
        F.max("total_revenue").alias("highest_revenue"),
        F.min("total_revenue").alias("lowest_revenue")
    ).show()

if __name__ == "__main__":
    spark = get_spark_session(app_name="GoldLayerAggregation")
    try:
        aggregate_sales_data(spark)
    finally:
        spark.stop()
        print("SparkSession stopped.")
