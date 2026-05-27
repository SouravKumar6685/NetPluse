#!/usr/bin/env python3
"""
src/track3_streaming/cdr_producer.py
Simulates live 5G mobility sessions (Voice and Data) and streams logs
directly into the Microsoft Fabric Eventstream ingress layer.
"""

import os
import sys
import json
import time
import uuid
import random
from datetime import datetime, timezone
from dotenv import load_dotenv
from azure.eventhub import EventHubProducerClient, EventData

# Pull active cloud environment variables
load_dotenv()
CONNECTION_STRING = os.getenv("FABRIC_CDR_CONNECTION_STRING")

if not CONNECTION_STRING or "your-namespace" in CONNECTION_STRING:
    print("CRITICAL: Ingress tracking connection string missing in local .env configuration.")
    sys.exit(1)

# Localized configuration arrays ensuring data architecture convergence
CIRCLES = ["AP_TELANGANA", "KARNATAKA", "MAHARASHTRA", "MUMBAI_METRO", "DELHI_NCR"]
SESSION_TYPES = ["VOICE", "DATA", "SMS"]
REASON_CODES = ["NORMAL", "DROP_HANDOVER_FAILURE", "DROP_CONGESTION", "DROP_LOW_RSRP"]

def generate_mock_cdr_event():
    """Synthesizes an active, granular multi-tier data or voice session footprint."""
    circle = random.choice(CIRCLES)
    session_type = random.choices(SESSION_TYPES, weights=[0.3, 0.6, 0.1], k=1)[0]
    
    # Generate mock network parameters
    duration = 0 if session_type == "SMS" else random.randint(10, 720)
    data_bytes = random.randint(1024, 157286400) if session_type == "DATA" else 0 # up to 150MB pings
    
    # Establish infrastructure context markers
    tower_index = random.randint(0, 39)
    tower_id = f"TWR_{circle}_{tower_index:03d}"
    sector_char = random.choice(["A", "B", "C"])
    sector_id = f"{tower_id}_SEC_{sector_char}"
    
    # Enforce realistic failure drops across weak coverage areas
    reason_code = "NORMAL"
    if session_type != "SMS" and random.random() < 0.08:
        reason_code = random.choice(REASON_CODES[1:])

    payload = {
        "session_id": str(uuid.uuid4()),
        "subscriber_id": str(uuid.uuid4()),  # Relational foreign key
        "session_type": session_type,
        "telecom_circle": circle,
        "duration_seconds": duration,
        "data_volume_bytes": data_bytes,
        "tower_id": tower_id,
        "sector_id": sector_id,
        "reason_code": reason_code,
        "event_timestamp": datetime.now(timezone.utc).isoformat()
    }
    return payload

def run_cdr_stream():
    print("Initializing production Event Hub engine for CDR streaming...")
    print("Target Client: Microsoft Fabric Eventstream Interface Connection")
    
    # Initialize the synchronous Event Hub pipeline executor
    client = EventHubProducerClient.from_connection_string(conn_str=CONNECTION_STRING)
    
    try:
        with client:
            print("[ONLINE] Live streaming initiated. Transmitting events... (Press Ctrl+C to stop)")
            while True:
                # Group individual sessions into micro-batches to compress protocol overhead
                event_data_batch = client.create_batch()
                batch_size = random.randint(3, 8)
                
                simulated_records = []
                for _ in range(batch_size):
                    raw_event = generate_mock_cdr_event()
                    serialized_json = json.dumps(raw_event)
                    
                    # Package structured string payload into EventHub framing envelope
                    event_data = EventData(serialized_json)
                    event_data_batch.add(event_data)
                    simulated_records.append(raw_event)
                
                # Push the compressed tracking token array to Microsoft Fabric OneLake
                client.send_batch(event_data_batch)
                
                # Clear logging trail trace
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"[{timestamp}] Dispatched batch of {len(simulated_records)} records to Fabric. "
                      f"Last Type: {simulated_records[-1]['session_type']} | Circle: {simulated_records[-1]['telecom_circle']}")
                
                # Introduce variable polling delay mimicking network peaks and valleys
                time.sleep(random.uniform(1.5, 4.0))
                
    except KeyboardInterrupt:
        print("\n[OFFLINE] Terminating call detail record generation loops safely.")
    except Exception as ex:
        print(f"\nFATAL STREAM INGESTION FAULT ENCOUNTERED: {ex}")

if __name__ == "__main__":
    run_cdr_stream()