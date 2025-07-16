import dlt
from pyspark.sql.functions import *

@dlt.table(comment="Raw sales data ingested from CSV.")
def bronze_sales():
    return (
        spark.readStream
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load("/data/raw/sales_data.csv") # Path relative to Databricks workspace or DBFS
    )


