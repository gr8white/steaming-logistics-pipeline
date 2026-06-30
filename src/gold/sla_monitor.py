from pyspark.sql.functions import col, current_timestamp, unix_timestamp, round as spark_round, lit, when

current_state = spark.read.table("rapidroute.silver.shipment_current_state")

sla_hours = {
    "express":  24,
    "standard": 48
}

sla_at_risk = (
    current_state
    # Only active shipments — not delivered or failed
    .filter(~col("status").isin(["DELIVERED", "FAILED_DELIVERY"]))
    # Exclude rows with no estimated delivery timestamp
    .filter(col("estimated_delivery_ts_parsed").isNotNull())
    .withColumn("hours_until_sla",
        spark_round(
            (unix_timestamp(col("estimated_delivery_ts_parsed")) - unix_timestamp(current_timestamp())) / 3600,
            2
        )
    )
    # At risk = SLA expires within the next 4 hours
    .filter(col("hours_until_sla") <= 4)
    # Also flag overdue (SLA already passed)
    .withColumn("sla_status",
        when(col("hours_until_sla") < 0, lit("OVERDUE"))
        .otherwise(lit("AT_RISK"))
    )
    .select(
        "shipment_id",
        "carrier_id",
        "status",
        "service_level",
        "destination_city",
        "customer_id",
        "estimated_delivery_ts_parsed",
        "hours_until_sla",
        "sla_status",
        "_last_updated"
    )
    .orderBy("hours_until_sla")
)

sla_at_risk.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("rapidroute.gold.sla_monitor")

print(f"SLA monitor updated: {sla_at_risk.count()} shipments at risk or overdue")