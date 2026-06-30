# RapidRoute Streaming Logistics Pipeline

A production-style streaming data pipeline built on Databricks Free Edition. The project simulates a real client engagement: RapidRoute, a last-mile delivery company, needs real-time shipment tracking to catch SLA breaches before the delivery window closes — something their overnight batch process can't do.

This project builds an end-to-end streaming pipeline: synthetic event files land in a Unity Catalog Volume, Auto Loader detects and ingests them incrementally, streaming transforms clean and maintain a stateful current-state table via Delta MERGE, and a gold layer surfaces at-risk shipments for the operations team. Delta maintenance patterns (OPTIMIZE, ZORDER, VACUUM, time travel) round out the project.

---

## Tools and Technologies

| Tool | Purpose |
|---|---|
| Databricks Free Edition | Workspace, serverless compute, Unity Catalog |
| Auto Loader (`cloudFiles`) | Incremental file detection and ingestion from Unity Catalog Volumes |
| Structured Streaming | Continuous stream processing with watermarking and `foreachBatch` |
| Delta Lake | Streaming sink, CDC upserts via MERGE, time travel, OPTIMIZE/ZORDER/VACUUM |
| PySpark | Streaming transforms, timestamp parsing, deduplication within micro-batches |
| Lakeflow Jobs | Orchestration of the full pipeline as a scheduled DAG |
| Declarative Automation Bundles (DABs) | Infrastructure-as-code deployment via `databricks.yml` |
| GitHub Actions | CI/CD — auto-deploy to prod on merge to `main` |

---

## Topics Covered

- **Auto Loader** — `cloudFiles` format, schema inference, schema hints, `mergeSchema`, checkpoint-based fault tolerance, schema evolution testing
- **Structured Streaming** — `readStream`/`writeStream`, `availableNow` trigger, watermarking for late data, `outputMode("append")`
- **`foreachBatch`** — running arbitrary Delta operations (MERGE) against each streaming micro-batch
- **Delta MERGE (CDC)** — stateful upsert pattern maintaining one row per shipment with conditional update logic
- **Within-batch deduplication** — using window functions inside `foreachBatch` to resolve multiple events for the same key in a single micro-batch
- **Gold aggregations** — derived metrics with `unix_timestamp`, conditional `when/otherwise`, `orderBy`
- **Delta maintenance** — `OPTIMIZE`, `ZORDER BY`, `DESCRIBE HISTORY`, `VERSION AS OF`, `TIMESTAMP AS OF`, `RESTORE`, `VACUUM`
- **Small file problem** — understanding why streaming writes create many small files and how OPTIMIZE compacts them

---

## Project Structure

```
streaming-logistics-pipeline/
├── databricks.yml                        # Bundle root config
├── resources/
│   └── rapids_job.yml                    # Lakeflow Job DAG definition
├── src/
│   ├── bronze/
│   │   └── ingest_events.py              # Auto Loader — JSON → bronze.shipment_events
│   ├── silver/
│   │   ├── stream_clean_events.py        # Streaming transform → silver.shipment_events_clean
│   │   └── stream_current_state.py       # MERGE stream → silver.shipment_current_state
│   ├── gold/
│   │   ├── sla_monitor.py                # At-risk shipments gold table
│   │   └── carrier_performance.py        # Weekly carrier delivery success rates
│   └── utils/
│       └── generate_events.py            # Synthetic shipment event generator
└── README.md
```

---

## How to Rebuild This Project

### Prerequisites

- [Databricks Free Edition account](https://www.databricks.com/try-databricks)
- [Databricks CLI v1.4+](https://docs.databricks.com/dev-tools/cli/install.html)
- Git and a GitHub account

---

### Step 1 — Workspace and Catalog Setup

Create the Unity Catalog structure and the Volume that receives incoming event files:

```sql
CREATE CATALOG IF NOT EXISTS rapidroute;
CREATE SCHEMA IF NOT EXISTS rapidroute.bronze;
CREATE SCHEMA IF NOT EXISTS rapidroute.silver;
CREATE SCHEMA IF NOT EXISTS rapidroute.gold;
CREATE VOLUME IF NOT EXISTS rapidroute.bronze.raw_events;
CREATE VOLUME IF NOT EXISTS rapidroute.bronze.checkpoints;
```

---

### Step 2 — Clone the Repo and Connect Git Folders

```bash
git clone <your-repo-url>
```

In your Databricks workspace: **Workspace → Add → Git folder** → paste your repo URL → set branch to `main`.

---

### Step 3 — Seed Initial Data

Open a notebook, import the generator, and produce at least 3 batches:

```python
from src.utils.generate_events import generate_batch
generate_batch(n_shipments=200, batch_id=1)
generate_batch(n_shipments=150, batch_id=2)
generate_batch(n_shipments=175, batch_id=3)
```

Confirm the files are visible in `/Volumes/rapidroute/bronze/raw_events/`.

---

### Step 4 — Run the Pipeline

Execute each script in order. Each uses `trigger(availableNow=True)` — it processes all available data and stops, so you can run them sequentially in notebook cells:

```python
exec(open("./src/bronze/ingest_events.py").read())
exec(open("./src/silver/stream_clean_events.py").read())
exec(open("./src/silver/stream_current_state.py").read())
exec(open("./src/gold/sla_monitor.py").read())
```

---

### Step 5 — Configure and Deploy the DAB

Update `databricks.yml` with your workspace host and email. Install the CLI and authenticate:

```bash
databricks configure
databricks bundle validate
databricks bundle deploy --target prod
```

Set up GitHub secrets (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`) for the Actions workflow. Any push to `main` auto-deploys to prod.

---

## Testing the MERGE Behavior

To verify the CDC upsert works correctly, inject a synthetic event for a known shipment ID with a newer timestamp and confirm the row updates without duplicating:

```python
import json
from datetime import datetime, timedelta
from pyspark.sql.functions import col

shipment_id = spark.read.table("rapidroute.silver.shipment_current_state") \
    .filter(col("status") == "CREATED").orderBy(rand()).first()["shipment_id"]

# Inject a newer event
with open("/Volumes/rapidroute/bronze/raw_events/test_event.json", "w") as f:
    f.write(json.dumps({
        "event_id": "test-001", "shipment_id": shipment_id,
        "carrier_id": "carrier_001", "status": "delivered",
        "event_timestamp": (datetime.utcnow() + timedelta(hours=1)).isoformat(),
        "origin_city": "São Paulo", "destination_city": "Curitiba",
        "service_level": "express",
        "estimated_delivery_ts": datetime.utcnow().isoformat(),
        "weight_kg": 5.0, "customer_id": "cust_9999"
    }))

exec(open("./src/bronze/ingest_events.py").read())
exec(open("./src/silver/stream_clean_events.py").read())
exec(open("./src/silver/stream_current_state.py").read())

spark.read.table("rapidroute.silver.shipment_current_state") \
    .filter(col("shipment_id") == shipment_id).show(truncate=False)
```

---

## Databricks Free Edition Notes

- **`trigger(processingTime=...)`** is not supported on serverless compute — use `availableNow=True` only
- Streaming checkpoints must live in a Unity Catalog Volume or DBFS; use `/Volumes/rapidroute/bronze/checkpoints/`
- Clean restarts require deleting both the raw files and the checkpoint directories, then dropping the tables