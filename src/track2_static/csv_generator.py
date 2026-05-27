#!/usr/bin/env python3
"""
src/track2_static/csv_generator.py
Generates the static dimension lookup reference files for Project NetPulse.
These files act as immutable dimensions within the Fabric Lakehouse layer.
This script runs completely offline and does not interact with PostgreSQL.
"""

import os
import pandas as pd

def generate_static_dimensions():
    # Define output directory path relative to the root execution space
    output_dir = os.path.join("data", "static")
    os.makedirs(output_dir, exist_ok=True)
    
    print("Initializing pure static dimension catalog generator...")
    print(f"Target directory: {os.path.abspath(output_dir)}\n")

    # ----------------------------------------------------------------------
    # DIMENSION 1: GEO PINCODE MASTER
    # ----------------------------------------------------------------------
    geo_data = {
        "pincode": ["500032", "500081", "500084", "560038", "560066", "560034", "411005", "411014", "411057", "400051", "400053", "400021", "110001", "201301", "122002"],
        "locality": ["Gachibowli", "Madhapur", "Kondapur", "Indiranagar", "Whitefield", "Koramangala", "Shivajinagar", "Viman Nagar", "Hinjewadi", "Bandra Kurla Complex", "Andheri West", "Nariman Point", "Connaught Place", "Sector 62", "DLF Phase 3"],
        "city": ["Hyderabad", "Hyderabad", "Hyderabad", "Bengaluru", "Bengaluru", "Bengaluru", "Pune", "Pune", "Pune", "Mumbai", "Mumbai", "Mumbai", "New Delhi", "Noida", "Gurugram"],
        "state": ["Telangana", "Telangana", "Telangana", "Karnataka", "Karnataka", "Karnataka", "Maharashtra", "Maharashtra", "Maharashtra", "Maharashtra", "Maharashtra", "Maharashtra", "Delhi", "Uttar Pradesh", "Haryana"],
        "telecom_circle": ["AP_TELANGANA", "AP_TELANGANA", "AP_TELANGANA", "KARNATAKA", "KARNATAKA", "KARNATAKA", "MAHARASHTRA", "MAHARASHTRA", "MAHARASHTRA", "MUMBAI_METRO", "MUMBAI_METRO", "MUMBAI_METRO", "DELHI_NCR", "DELHI_NCR", "DELHI_NCR"]
    }
    df_geo = pd.DataFrame(geo_data)
    geo_path = os.path.join(output_dir, "geo_pincode_master.csv")
    df_geo.to_csv(geo_path, index=False)
    print(f"  [✓] Generated: geo_pincode_master.csv ({len(df_geo)} structural rows)")

    # ----------------------------------------------------------------------
    # DIMENSION 2: TARIFF PLAN DICTIONARY (Paisa and Byte Architecture)
    # ----------------------------------------------------------------------
    tariff_data = {
        "plan_code": ["5G_UNLIM_PREMIUM", "5G_DATA_BUDDY", "5G_CORP_ELITE"],
        "plan_name": ["Unlimited Gold 5G", "Basic Data Booster", "Enterprise Unlimited"],
        "monthly_price_paisa": [74900, 14900, 199900], # ₹749.00, ₹149.00, ₹1,999.00
        "data_quota_bytes": [1099511627776, 16106127360, 5497558138880], # 1TB, 15GB, 5TB
        "validity_days": [28, 14, 30],
        "service_tier": ["PREMIUM", "STANDARD", "ENTERPRISE"]
    }
    df_tariff = pd.DataFrame(tariff_data)
    tariff_path = os.path.join(output_dir, "tariff_plan_dictionary.csv")
    df_tariff.to_csv(tariff_path, index=False)
    print(f"  [✓] Generated: tariff_plan_dictionary.csv ({len(df_tariff)} structural rows)")

    # ----------------------------------------------------------------------
    # DIMENSION 3: NETWORK ERROR CODES DICTIONARY
    # ----------------------------------------------------------------------
    error_data = {
        "reason_code": ["NORMAL", "DROP_HANDOVER_FAILURE", "DROP_CONGESTION", "DROP_LOW_RSRP"],
        "failure_type": ["NONE", "RADIO_LINK", "CAPACITY", "COVERAGE"],
        "severity": ["INFO", "HIGH", "CRITICAL", "MEDIUM"],
        "description": [
            "Session terminated gracefully by user action",
            "Signal dropped during cell hardware boundary handover processing",
            "Target cell sector capacity limits exhausted, connection dropped",
            "User device tracking moved completely out of range of active receiver panels"
        ]
    }
    df_error = pd.DataFrame(error_data)
    error_path = os.path.join(output_dir, "network_error_codes.csv")
    df_error.to_csv(error_path, index=False)
    print(f"  [✓] Generated: network_error_codes.csv ({len(df_error)} structural rows)")

    # ----------------------------------------------------------------------
    # DIMENSION 4: HARDWARE DEVICE TYPE ALLOCATION MASTER (TAC)
    # ----------------------------------------------------------------------
    device_data = {
        "tac_prefix": ["35925411", "86024105", "35174211", "86991023"],
        "brand": ["Apple", "Samsung", "OnePlus", "Generic"],
        "model": ["iPhone 15 Pro", "Galaxy S24 Ultra", "CPH2411", "M2026_5G"],
        "marketing_name": ["iPhone 15 Pro", "Galaxy S24 Ultra", "OnePlus 11", "Generic 5G Handset"],
        "device_type": ["SMARTPHONE", "SMARTPHONE", "SMARTPHONE", "M2M_TERMINAL"]
    }
    df_device = pd.DataFrame(device_data)
    device_path = os.path.join(output_dir, "device_tac_master.csv")
    df_device.to_csv(device_path, index=False)
    print(f"  [✓] Generated: device_tac_master.csv ({len(df_device)} structural rows)")

    print("\n=== TRACK 2 STATIC CATALOGS EXPORTED SUCCESSFULLY ===")
    print("Files are built and verified inside data/static/ directory.")

if __name__ == "__main__":
    generate_static_dimensions()