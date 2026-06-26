from pyspark.sql.functions import (
    col, to_timestamp, upper, trim, when, lit, current_timestamp
)

CHECKPOINT   = "/Volumes/rapidroute/bronze/checkpoints/silver_clean"
TARGET_TABLE = "rapidroute.silver.shipment_events_clean"

VALID_STATUSES = ["created", "picked_up", "in_transit",
                  "out_for_delivery", "delivered", "failed_delivery"]

TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"

bronze_stream = (
    spark.readStream
    .format("delta")
    .table("rapidroute.bronze.shipment_events")
)

# Parse and clean
clean_stream = (
    bronze_stream
    .withColumn("event_timestamp_parsed",
        to_timestamp(col("event_timestamp"), TIMESTAMP_FORMAT))
    .withColumn("estimated_delivery_ts_parsed",
        to_timestamp(col("estimated_delivery_ts"), TIMESTAMP_FORMAT))
    # Watermark on event_timestamp: drop events more than 30 minutes late.
    # This bounds the state Spark keeps in memory for stateful operations.
    .withWatermark("event_timestamp_parsed", "30 minutes")
    .withColumn("status", trim(upper(col("status"))))
    # Normalize status values — carrier data is inconsistent
    .withColumn("status",
        when(col("status").isin([s.upper() for s in VALID_STATUSES]), col("status"))
        .otherwise(lit("UNKNOWN"))
    )
    # Drop rows missing the fields we can't recover
    .filter(col("shipment_id").isNotNull())
    .filter(col("event_id").isNotNull())
    .filter(col("event_timestamp_parsed").isNotNull())
    .withColumn("_processed_at", current_timestamp())
    .drop("event_timestamp", "estimated_delivery_ts")  # replaced by parsed versions
)

(
    clean_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE)
)