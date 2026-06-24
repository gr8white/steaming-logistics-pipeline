import json
import uuid
import random
from datetime import datetime, timedelta
from pathlib import Path

VOLUME_PATH = "/Volumes/rapidroute/bronze/raw_events"
CITIES = ["São Paulo", "Rio de Janeiro", "Brasília", "Salvador", "Fortaleza", "Curitiba", "Manaus", "Recife"]
CARRIERS = ["carrier_001", "carrier_002", "carrier_003", "carrier_004"]
STATUSES = ["created", "picked_up", "in_transit", "out_for_delivery", "delivered", "failed_delivery"]

def generate_batch(n_shipments=50, batch_id=1):
    events = []
    now = datetime.utcnow()

    for _ in range(n_shipments):
        shipment_id = str(uuid.uuid4())[:12]
        service_level = random.choice(["standard", "express"])
        sla_hours = 24 if service_level == "express" else 48
        created_ts = now - timedelta(hours=random.randint(1, sla_hours + 10))
        estimated_delivery_ts = created_ts + timedelta(hours=sla_hours)

        # Each shipment gets 1-4 status updates in sequence
        status_sequence = STATUSES[:random.randint(1, 5)]
        for i, status in enumerate(status_sequence):
            events.append({
                "event_id": str(uuid.uuid4()),
                "shipment_id": shipment_id,
                "carrier_id": random.choice(CARRIERS),
                "status": status,
                "event_timestamp": (created_ts + timedelta(hours=i * 3)).isoformat(),
                "origin_city": random.choice(CITIES),
                "destination_city": random.choice(CITIES),
                "service_level": service_level,
                "estimated_delivery_ts": estimated_delivery_ts.isoformat(),
                "weight_kg": round(random.uniform(0.5, 30.0), 2),
                "customer_id": f"cust_{random.randint(1000, 9999)}",
                "region": random.choice(["North", "South", "East", "West"])
            })

    path = Path(VOLUME_PATH)
    output_file = path / f"events_batch_{batch_id:04d}.json"
    with open(output_file, "w") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")

    print(f"Wrote {len(events)} events to {output_file}")