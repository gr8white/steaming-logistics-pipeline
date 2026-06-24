from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType

VOLUME_PATH   = "/Volumes/rapidroute/bronze/raw_events"
CHECKPOINT    = "/Volumes/rapidroute/bronze/checkpoints/ingest_events"
TARGET_TABLE  = "rapidroute.bronze.shipment_events"

# Schema hints tell Auto Loader how to cast specific fields.
# Without this, event_timestamp and estimated_delivery_ts would be inferred as strings —
# which is fine for bronze (we'll parse them properly in silver), but being explicit
# about weight_kg prevents it from being read as a long integer.
schema_hints = "weight_kg FLOAT, event_id STRING, shipment_id STRING"

stream = (
    spark.readStream
    .format("cloudFiles")                         # Auto Loader format
    .option("cloudFiles.format", "json")          # underlying file format
    .option("cloudFiles.schemaLocation", CHECKPOINT + "/schema")  # where to store inferred schema
    .option("cloudFiles.schemaHints", schema_hints)
    .option("cloudFiles.inferColumnTypes", "true")
    .load(VOLUME_PATH)
    .withColumn("_source_file", col("_metadata.file_path"))
    .withColumn("_ingested_at", current_timestamp())
)

(
    stream.writeStream
    .format("delta")
    .outputMode("append")                         # new rows only — correct for event streams
    .option("checkpointLocation", CHECKPOINT)
    .option("mergeSchema", "true")                # allow schema evolution if carriers add fields
    .trigger(availableNow=True)                   # process all available files, then stop
    .toTable(TARGET_TABLE)
)