from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from conf.spark_session_config import get_spark_session

def transform_sales_data(spark: SparkSession, input_path="../data/bronze/sales", output_path="../data/silver/sales"):
    """
    Reads sales data from Bronze layer, transforms it, and writes to Silver layer.
    """
    print(f"Reading data from Bronze layer at {input_path}...")
    df = spark.read.format("delta").load(input_path)

    print("Applying transformations...")
    transformed_df = df.withColumn("total_price", col("quantity") * col("price")) \
                       .withColumn("processing_timestamp", current_timestamp())

    print(f"Writing transformed data to Silver layer at {output_path}...")
    transformed_df.write.format("delta").mode("overwrite").save(output_path)
    print("Silver layer transformation complete.")

if __name__ == "__main__":
    spark = get_spark_session(app_name="SilverLayerTransformation")
    transform_sales_data(spark)
    spark.stop()

