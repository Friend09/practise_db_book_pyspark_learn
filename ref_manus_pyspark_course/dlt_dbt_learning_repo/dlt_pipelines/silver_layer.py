import dlt
from pyspark.sql.functions import col, current_timestamp

@dlt.table(comment="Cleaned and enriched sales data.")
def silver_sales():
    return (
        dlt.read("bronze_sales")
            .withColumn("total_price", col("quantity") * col("price"))
            .withColumn("processing_timestamp", current_timestamp())
    )


