from pyspark.sql.functions import col, date_trunc, count, when, round as spark_round

events_clean = spark.read.table("rapidroute.silver.shipment_events_clean")

carrier_perf = (
    events_clean
    .filter(col("status").isin(["DELIVERED", "FAILED_DELIVERY"]))
    .groupBy(
        date_trunc("week", col("event_timestamp_parsed")).alias("week"),
        col("carrier_id"),
        col("service_level")
    )
    .agg(
        count(when(col("status") == "DELIVERED", 1)).alias("delivered_count"),
        count(when(col("status") == "FAILED_DELIVERY", 1)).alias("failed_count"),
        count("*").alias("total_completed")
    )
    .withColumn("delivery_success_rate",
        spark_round(col("delivered_count") / col("total_completed") * 100, 2)
    )
    .orderBy("week", "carrier_id")
)

carrier_perf.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("rapidroute.gold.carrier_performance")