import os
import sys

# Add the project root to the Python path to import custom modules
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if project_root not in sys.path:
    sys.path.append(project_root)

from conf.spark_session_config import get_spark_session

def ingest_raw_sales_data(spark, input_path="data/sales_data.csv", output_path="data/bronze/sales"):
    """
    Reads raw sales data from CSV and writes it to a Parquet table in the Bronze layer.
    """
    print(f"Reading raw data from {input_path}...")

    # Check if input file exists
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    df = spark.read.csv(input_path, header=True, inferSchema=True)

    print(f"Data schema:")
    df.printSchema()

    print(f"Number of records: {df.count()}")

    print(f"Writing data to Bronze layer at {output_path}...")

    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Write as Parquet instead of Delta to avoid JAR issues
    df.write.mode("overwrite").parquet(output_path)

    print("Bronze layer ingestion complete.")

    # Show sample data
    print("Sample data:")
    df.show(5)

if __name__ == "__main__":
    spark = get_spark_session(app_name="BronzeLayerIngestion")
    try:
        ingest_raw_sales_data(spark)
    finally:
        spark.stop()
        print("SparkSession stopped.")
