from pyspark.sql import SparkSession

def get_spark_session(app_name="PySparkLearningApp", master="local[*]"):
    """
    Initializes and returns a SparkSession with common configurations.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .master(master) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
        .getOrCreate()
    return spark

if __name__ == "__main__":
    spark = get_spark_session()
    print("SparkSession created successfully!")
    print(f"Spark Version: {spark.version}")
    print(f"Application Name: {spark.conf.get('spark.app.name')}")
    spark.stop()
    print("SparkSession stopped.")

