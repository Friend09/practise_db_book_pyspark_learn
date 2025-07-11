from pyspark.sql import SparkSession
from conf.spark_session_config import get_spark_session

def ingest_raw_sales_data(spark: SparkSession, input_path="../data/raw/sales_data.csv", output_path="../data/bronze/sales"):
    """
    Reads raw sales data from CSV and writes it to a Delta table in the Bronze layer.
    """
    print(f"Reading raw data from {input_path}...")
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    print(f"Writing data to Bronze layer at {output_path}...")
    df.write.format("delta").mode("overwrite").save(output_path)
    print("Bronze layer ingestion complete.")

if __name__ == "__main__":
    spark = get_spark_session(app_name="BronzeLayerIngestion")
    ingest_raw_sales_data(spark)
    spark.stop()

