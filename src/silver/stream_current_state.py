from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp, row_number, max as spark_max
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

CHECKPOINT   = "/Volumes/rapidroute/bronze/checkpoints/current_state"
TARGET_TABLE = "rapidroute.silver.shipment_current_state"

def upsert_to_current_state(batch_df: DataFrame, batch_id: int):
    """
    Called for each micro-batch. Deduplicates within the batch first —
    if the same shipment appears multiple times in one batch (e.g., two files
    landed with events for the same shipment), we only want the most recent.
    Then we MERGE against the target table.
    """
    if batch_df.isEmpty():
        return

    # Step 1: within this batch, keep only the latest event per shipment
    # A single micro-batch can contain multiple events for the same shipment
    # from different files. We need to resolve to one row before merging.
    window = Window.partitionBy("shipment_id").orderBy(col("event_timestamp_parsed").desc())
    latest_in_batch = (
        batch_df
        .withColumn("rn", row_number().over(window))
        .filter(col("rn") == 1)
        .drop("rn")
        .withColumn("_last_updated", current_timestamp())
    )

    # Step 2: MERGE the deduplicated batch into the target table
    target = DeltaTable.forName(spark, TARGET_TABLE)

    (
        target.alias("t")
        .merge(
            latest_in_batch.alias("s"),
            "t.shipment_id = s.shipment_id"
        )
        # Only update if the incoming event is newer than what we have
        .whenMatchedUpdate(
            condition="s.event_timestamp_parsed > t.event_timestamp_parsed",
            set={
                "carrier_id":                   "s.carrier_id",
                "status":                       "s.status",
                "service_level":                "s.service_level",
                "origin_city":                  "s.origin_city",
                "destination_city":             "s.destination_city",
                "event_timestamp_parsed":       "s.event_timestamp_parsed",
                "estimated_delivery_ts_parsed": "s.estimated_delivery_ts_parsed",
                "weight_kg":                    "s.weight_kg",
                "customer_id":                  "s.customer_id",
                "_last_updated":                "s._last_updated"
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

clean_stream = (
    spark.readStream
    .format("delta")
    .table("rapidroute.silver.shipment_events_clean")
)

(
    clean_stream.writeStream
    .foreachBatch(upsert_to_current_state)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .start()
)