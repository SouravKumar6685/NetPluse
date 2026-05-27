#!/usr/bin/env python3
"""
src/track3_streaming/telemetry_producer.py
Generates continuous performance and hardware status signals from cell sectors
and uploads logs directly into the cloud processing layer.
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
CONNECTION_STRING = os.getenv("FABRIC_TELEMETRY_CONNECTION_STRING")

if not CONNECTION_STRING or "your-namespace" in CONNECTION_STRING:
    print("CRITICAL: Telemetry routing tracking connection string missing in local .env context configuration.")
    sys.exit(1)

CIRCLES = ["AP_TELANGANA", "KARNATAKA", "MAHARASHTRA", "MUMBAI_METRO", "DELHI_NCR"]

def generate_hardware_metrics():
    """Computes operational state telemetry signals across virtualized physical assets."""
    circle = random.choice(CIRCLES)
    tower_index = random.randint(0, 39)
    tower_id = f"TWR_{circle}_{i:03d}" if 'i' in locals() else f"TWR_{circle}_{tower_index:03d}"
    sector_char = random.choice(["A", "B", "C"])
    sector_id = f"{tower_id}_SEC_{sector_char}"
    
    # Synthesize telemetry distributions based on typical hardware profiles
    cpu_util = round(random.uniform(15.5, 98.2), 2)
    active_conn = random.randint(45, 1200)
    
    # Reference Signal Received Power metric (Closer to 0 is stronger, -115+ indicates structural dropouts)
    rsrp_dbm = random.randint(-118, -65)
    
    # Packet delivery degradation drops increase as hardware bottlenecks escalate
    packet_drop_rate = round(random.uniform(0.00, 1.25), 3) if cpu_util < 85 else round(random.uniform(2.10, 8.45), 3)

    payload = {
        "telemetry_id": str(uuid.uuid4()),
        "tower_id": tower_id,
        "sector_id": sector_id,
        "cpu_utilization_percent": cpu_util,
        "active_connections_count": active_conn,
        "rsrp_signal_strength_dbm": rsrp_dbm,
        "packet_drop_rate_percent": packet_drop_rate,
        "telemetry_timestamp": datetime.now(timezone.utc).isoformat()
    }
    return payload

def run_telemetry_stream():
    print("Initializing production Event Hub engine for Tower Telemetry tracking...")
    print("Target Client: Microsoft Fabric Eventstream Interface Connection")
    
    client = EventHubProducerClient.from_connection_string(conn_str=CONNECTION_STRING)
    
    try:
        with client:
            print("[ONLINE] Live hardware analytics tracking active. Transmitting events... (Press Ctrl+C to stop)")
            while True:
                event_data_batch = client.create_batch()
                batch_size = random.randint(2, 5)
                
                simulated_records = []
                for _ in range(batch_size):
                    raw_event = generate_hardware_metrics()
                    serialized_json = json.dumps(raw_event)
                    
                    event_data = EventData(serialized_json)
                    event_data_batch.add(event_data)
                    simulated_records.append(raw_event)
                
                client.send_batch(event_data_batch)
                
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"[{timestamp}] Dispatched telemetry signals for {len(simulated_records)} sectors to Fabric. "
                      f"Last Scanned: {simulated_records[-1]['sector_id']} | Load: {simulated_records[-1]['cpu_utilization_percent']}%")
                
                # Telemetry checks poll on stable periodic clock cycles
                time.sleep(2.0)
                
    except KeyboardInterrupt:
        print("\n[OFFLINE] Terminating infrastructure diagnostic logging safely.")
    except Exception as ex:
        print(f"\nFATAL INFRASTRUCTURE TELEMETRY EXCEPTION ENCOUNTERED: {ex}")

if __name__ == "__main__":
    run_telemetry_stream()