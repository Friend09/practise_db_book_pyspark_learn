from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col
from conf.spark_session_config import get_spark_session

def aggregate_sales_data(spark: SparkSession, input_path="../data/silver/sales", output_path="../data/gold/sales_summary"):
    """
    Reads sales data from Silver layer, aggregates it, and writes to Gold layer.
    """
    print(f"Reading data from Silver layer at {input_path}...")
    df = spark.read.format("delta").load(input_path)

    print("Aggregating data...")
    aggregated_df = df.groupBy("product_name", "city") \
                        .agg(sum("total_price").alias("total_revenue")) \
                        .orderBy(col("total_revenue").desc())

    print(f"Writing aggregated data to Gold layer at {output_path}...")
    aggregated_df.write.format("delta").mode("overwrite").save(output_path)
    print("Gold layer aggregation complete.")

if __name__ == "__main__":
    spark = get_spark_session(app_name="GoldLayerAggregation")
    aggregate_sales_data(spark)
    spark.stop()

