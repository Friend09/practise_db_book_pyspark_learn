from pyspark.sql import SparkSession

def get_spark_session(app_name="PySparkLearningApp", master="local[*]"):
    """
    Initializes and returns a SparkSession with common configurations.
    This version avoids Delta Lake dependencies to prevent JAR issues.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .master(master) \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()
    return spark

if __name__ == "__main__":
    spark = get_spark_session()
    print("SparkSession created successfully!")
    print(f"Spark Version: {spark.version}")
    print(f"Application Name: {spark.conf.get('spark.app.name')}")
    spark.stop()
    print("SparkSession stopped.")
