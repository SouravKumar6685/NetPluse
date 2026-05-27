#!/usr/bin/env python3
"""
src/track1_batch/db_initializer.py
Establishes the enterprise relational schema for IndiConnect 5G (Project NetPulse)
and populates it with raw, unmasked, localized Indian data pools.
Supports both clean-slate initialization and progressive live incremental seeding.
"""

import os
import uuid
import random
import string
from datetime import datetime, timezone, timedelta
import psycopg2
from faker import Faker
from dotenv import load_dotenv

# Load local environment configuration keys
load_dotenv()

# ----------------------------------------------------------------------
# 1. ENVIRONMENT CONFIGURATION COUPLING
# ----------------------------------------------------------------------
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "admin"),
    "dbname": os.getenv("DB_NAME", "netpulse_oltp")
}

# CONTROLLER FLAGS: Set APPEND_MODE to True to stack data dynamically across runs
APPEND_MODE = os.getenv("APPEND_MODE", "True").lower() == "true"

# ----------------------------------------------------------------------
# 2. LOCALIZED GEOGRAPHIC & INFRASTRUCTURE DICTIONARIES
# ----------------------------------------------------------------------
CIRCLES = ["AP_TELANGANA", "KARNATAKA", "MAHARASHTRA", "MUMBAI_METRO", "DELHI_NCR"]

GEO_MATRIX = {
    "AP_TELANGANA": [
        {"city": "Hyderabad", "locality": "Gachibowli", "pincode": "500032", "latitude": 17.4483, "longitude": 78.3741},
        {"city": "Hyderabad", "locality": "Madhapur", "pincode": "500081", "latitude": 17.4504, "longitude": 78.3812},
        {"city": "Hyderabad", "locality": "Kondapur", "pincode": "500084", "latitude": 17.4622, "longitude": 78.3568}
    ],
    "KARNATAKA": [
        {"city": "Bengaluru", "locality": "Indiranagar", "pincode": "560038", "latitude": 12.9718, "longitude": 77.6411},
        {"city": "Bengaluru", "locality": "Whitefield", "pincode": "560066", "latitude": 12.9698, "longitude": 77.7499},
        {"city": "Bengaluru", "locality": "Koramangala", "pincode": "560034", "latitude": 12.9352, "longitude": 77.6244}
    ],
    "MAHARASHTRA": [
        {"city": "Pune", "locality": "Shivajinagar", "pincode": "411005", "latitude": 18.5314, "longitude": 73.8446},
        {"city": "Pune", "locality": "Viman Nagar", "pincode": "411014", "latitude": 18.5679, "longitude": 73.9143},
        {"city": "Pune", "locality": "Hinjewadi", "pincode": "411057", "latitude": 18.5913, "longitude": 73.7389}
    ],
    "MUMBAI_METRO": [
        {"city": "Mumbai", "locality": "Bandra Kurla Complex", "pincode": "400051", "latitude": 19.0601, "longitude": 72.8634},
        {"city": "Mumbai", "locality": "Andheri West", "pincode": "400053", "latitude": 19.1363, "longitude": 72.8276},
        {"city": "Mumbai", "locality": "Nariman Point", "pincode": "400021", "latitude": 18.9260, "longitude": 72.8226}
    ],
    "DELHI_NCR": [
        {"city": "New Delhi", "locality": "Connaught Place", "pincode": "110001", "latitude": 28.6304, "longitude": 77.2177},
        {"city": "Noida", "locality": "Sector 62", "pincode": "201301", "latitude": 28.6219, "longitude": 77.3639},
        {"city": "Gurugram", "locality": "DLF Phase 3", "pincode": "122002", "latitude": 28.4901, "longitude": 77.0912}
    ]
}

TELECOM_PREFIXES = {
    "AP_TELANGANA": ["6300", "7013", "8309", "9848", "9949", "7702", "8688"],
    "KARNATAKA": ["8884", "7022", "6360", "9845", "9900", "8123", "9886"],
    "MAHARASHTRA": ["7030", "8766", "6351", "9822", "9922", "8411", "9823"],
    "MUMBAI_METRO": ["9322", "8108", "7977", "9819", "9930", "8451", "9820"],
    "DELHI_NCR": ["9311", "8800", "7827", "9810", "9818", "8527", "9811"]
}

PLAN_CODES = ["5G_UNLIM_PREMIUM", "5G_DATA_BUDDY", "5G_CORP_ELITE"]
PLAN_QUOTAS = {
    "5G_UNLIM_PREMIUM": 1099511627776,  # 1 TB in Bytes
    "5G_DATA_BUDDY": 16106127360,       # 15 GB in Bytes
    "5G_CORP_ELITE": 5497558138880      # 5 TB in Bytes
}

TAC_PREFIXES = ["35925411", "86024105", "35174211", "86991023"]
OS_VARIANTS = ["IOS", "ANDROID_14", "ANDROID_13"]

# Setup Localized India Data Seed Engine
fake = Faker('en_IN')

# ----------------------------------------------------------------------
# 3. STATUTORY RULE SYSTEM GENERATORS (UNMASKED)
# ----------------------------------------------------------------------
def generate_indian_pan(last_name):
    """Generates an authentic raw Indian PAN matching standard statutory syntax."""
    chars3 = "".join(random.choices(string.ascii_uppercase, k=3))
    holder_status = "P"  # Individual Person
    last_name_char = last_name[0].upper() if (last_name and last_name[0].isalpha()) else "X"
    digits4 = "".join(random.choices(string.digits, k=4))
    check_char = random.choice(string.ascii_uppercase)
    return f"{chars3}{holder_status}{last_name_char}{digits4}{check_char}"

def generate_indian_phone(circle):
    """Generates an Indian phone number matching prefix routing spaces."""
    prefix = random.choice(TELECOM_PREFIXES[circle])
    remaining_len = 10 - len(prefix)
    suffix = "".join(random.choices(string.digits, k=remaining_len))
    return f"+91-{prefix}{suffix}"

def generate_hierarchical_address(circle):
    """Generates structural geometric addresses aligned with state bounds."""
    geo = random.choice(GEO_MATRIX[circle])
    pincode = geo["pincode"]
    flat_no = f"Flat No. {random.randint(101, 905)}, Building {random.randint(1, 20)}"
    street = f"{fake.street_name() if random.random() > 0.3 else 'Main Road'}"
    return f"{flat_no}, {street}, {geo['locality']}, {geo['city']}, {circle.replace('_', ' ').title()} - {pincode}"

def generate_raw_aadhaar():
    """Generates a raw, unmasked 12-digit Aadhaar number matching statutory rules."""
    first_digit = random.choice("23456789")  # Aadhaar numbers do not start with 0 or 1
    remaining_digits = "".join(random.choices(string.digits, k=11))
    return f"{first_digit}{remaining_digits}"

def generate_imsi():
    """Generates a 15-digit International Mobile Subscriber Identity for India."""
    mcc_mnc = random.choice(["40488", "40588"])  # 88 = IndiConnect core allocation
    subscriber_id = "".join(random.choices(string.digits, k=10))
    return f"{mcc_mnc}{subscriber_id}"

def generate_iccid():
    """Generates a unique 20-digit structural SIM chip tracking string."""
    return "8991" + "".join(random.choices(string.digits, k=16))

def generate_imei():
    """Generates a unique 15-digit Device Identification string using real TAC codes."""
    tac = random.choice(TAC_PREFIXES)
    serial = "".join(random.choices(string.digits, k=6))
    check_digit = str(random.randint(0, 9))
    return f"{tac}{serial}{check_digit}"

# ----------------------------------------------------------------------
# 4. DATABASE DDL SETUP EXECUTION
# ----------------------------------------------------------------------
def setup_database_schema(cursor):
    """Creates schemas and structured validation tables conditionally based on run-mode."""
    
    if not APPEND_MODE:
        print("Wiping environment schemas (Clean Slate Reset)...")
        cursor.execute("DROP SCHEMA IF EXISTS crm CASCADE;")
        cursor.execute("DROP SCHEMA IF EXISTS inventory CASCADE;")
        cursor.execute("DROP SCHEMA IF EXISTS billing CASCADE;")
        cursor.execute("DROP SCHEMA IF EXISTS network_infra CASCADE;")
    else:
        print("Ensuring target structures exist without data destruction (Incremental Mode)...")

    # Initialize Modules safely if missing
    cursor.execute("CREATE SCHEMA IF NOT EXISTS crm;")
    cursor.execute("CREATE SCHEMA IF NOT EXISTS inventory;")
    cursor.execute("CREATE SCHEMA IF NOT EXISTS billing;")
    cursor.execute("CREATE SCHEMA IF NOT EXISTS network_infra;")

    # 1. NETWORK INFRASTRUCTURE
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS network_infra.cell_towers (
            tower_id VARCHAR(30) PRIMARY KEY,
            latitude NUMERIC(9,6) NOT NULL,
            longitude NUMERIC(9,6) NOT NULL,
            telecom_circle VARCHAR(20) NOT NULL,
            operational_status VARCHAR(15) NOT NULL DEFAULT 'ONLINE',
            CONSTRAINT chk_status CHECK (operational_status IN ('ONLINE', 'MAINTENANCE', 'DEGRADED'))
        );
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS network_infra.tower_sectors (
            sector_id VARCHAR(40) PRIMARY KEY,
            tower_id VARCHAR(30) NOT NULL REFERENCES network_infra.cell_towers(tower_id) ON DELETE CASCADE,
            azimuth_degrees INT NOT NULL,
            frequency_band VARCHAR(20) NOT NULL,
            CONSTRAINT chk_azimuth CHECK (azimuth_degrees >= 0 AND azimuth_degrees < 360)
        );
    """)

    # 2. CRM MODULE
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS crm.subscribers (
            subscriber_id UUID PRIMARY KEY,
            first_name VARCHAR(50) NOT NULL,
            last_name VARCHAR(50) NOT NULL,
            phone_number VARCHAR(15) UNIQUE NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            telecom_circle VARCHAR(20) NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT chk_phone_format CHECK (phone_number ~ '^\\+91-[6-9][0-9]{9}$')
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS crm.subscriber_kyc (
            kyc_id UUID PRIMARY KEY,
            subscriber_id UUID NOT NULL REFERENCES crm.subscribers(subscriber_id) ON DELETE CASCADE,
            aadhaar_number VARCHAR(12) UNIQUE NOT NULL,
            pan_card VARCHAR(10) UNIQUE NOT NULL,
            verification_status VARCHAR(20) NOT NULL DEFAULT 'VERIFIED',
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT chk_aadhaar_format CHECK (aadhaar_number ~ '^[2-9][0-9]{11}$'),
            CONSTRAINT chk_pan_format CHECK (pan_card ~ '^[A-Z]{3}P[A-Z][0-9]{4}[A-Z]$')
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS crm.customer_analytics_segments (
            subscriber_id UUID PRIMARY KEY REFERENCES crm.subscribers(subscriber_id) ON DELETE CASCADE,
            customer_lifetime_value_paisa BIGINT NOT NULL DEFAULT 0,
            churn_risk_score NUMERIC(3,2) NOT NULL DEFAULT 0.00,
            current_experience_tier VARCHAR(10) NOT NULL DEFAULT 'BRONZE',
            CONSTRAINT chk_churn_bounds CHECK (churn_risk_score >= 0.00 AND churn_risk_score <= 1.00),
            CONSTRAINT chk_tier CHECK (current_experience_tier IN ('GOLD', 'SILVER', 'BRONZE'))
        );
    """)

    # 3. HARDWARE INVENTORY
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS inventory.sim_cards (
            sim_id UUID PRIMARY KEY,
            subscriber_id UUID NOT NULL REFERENCES crm.subscribers(subscriber_id) ON DELETE CASCADE,
            imsi VARCHAR(15) UNIQUE NOT NULL,
            iccid VARCHAR(20) UNIQUE NOT NULL,
            activation_status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT chk_imsi CHECK (imsi ~ '^(404|405)[0-9]{12}$')
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS inventory.device_registry (
            device_entry_id UUID PRIMARY KEY,
            subscriber_id UUID NOT NULL REFERENCES crm.subscribers(subscriber_id) ON DELETE CASCADE,
            imei VARCHAR(15) UNIQUE NOT NULL,
            is_5g_sa_capable BOOLEAN NOT NULL DEFAULT TRUE,
            operating_system VARCHAR(20) NOT NULL,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
    """)

    # 4. BILLING SYSTEMS
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS billing.accounts (
            account_id UUID PRIMARY KEY,
            subscriber_id UUID NOT NULL REFERENCES crm.subscribers(subscriber_id) ON DELETE CASCADE,
            billing_type VARCHAR(10) NOT NULL,
            account_balance_paisa BIGINT NOT NULL DEFAULT 0,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT chk_billing_type CHECK (billing_type IN ('PREPAID', 'POSTPAID'))
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS billing.payment_ledger (
            transaction_id UUID PRIMARY KEY,
            account_id UUID NOT NULL REFERENCES billing.accounts(account_id) ON DELETE CASCADE,
            amount_paisa BIGINT NOT NULL,
            payment_method VARCHAR(20) NOT NULL,
            payment_gateway VARCHAR(20) NOT NULL,
            transaction_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS billing.data_allowance_ledgers (
            ledger_id UUID PRIMARY KEY,
            account_id UUID NOT NULL REFERENCES billing.accounts(account_id) ON DELETE CASCADE,
            plan_code VARCHAR(20) NOT NULL,
            allocated_quota_bytes BIGINT NOT NULL,
            consumed_quota_bytes BIGINT NOT NULL DEFAULT 0,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT chk_quota_bounds CHECK (consumed_quota_bytes <= allocated_quota_bytes)
        );
    """)
    print("Database structures initialized safely.")

# ----------------------------------------------------------------------
# 5. DATA INGESTION ENGINE (POPULATION)
# ----------------------------------------------------------------------
def populate_production_data(cursor, num_subscribers=None, num_towers=40):
    """Populates relational hierarchies with LIVE rolling timestamps."""
    
    if num_subscribers is None:
        num_subscribers = random.randint(1000, 1600) if APPEND_MODE else 1200

    print(f"Executing data generation footprint. Targeted batch additions: {num_subscribers} subscribers...")

    # FIXED: Hardcoded past parameters completely dropped. Time metrics are now computed LIVE.
    current_live_time = datetime.now(timezone.utc)
    
    cursor.execute("SELECT COUNT(*) FROM network_infra.cell_towers;")
    existing_towers = cursor.fetchone()[0]

    # -- 1. GENERATE NETWORK INFRASTRUCTURE ASSETS --
    if existing_towers == 0:
        print(f"Seeding {num_towers} infrastructure cell tower topologies...")
        towers = []
        sectors = []
        for i in range(num_towers):
            circle = random.choice(CIRCLES)
            geo = random.choice(GEO_MATRIX[circle])
            t_id = f"TWR_{circle}_{i:03d}"
            
            lat = float(geo["latitude"]) + random.uniform(-0.015, 0.015)
            lon = float(geo["longitude"]) + random.uniform(-0.015, 0.015)
            status = "ONLINE" if random.random() > 0.05 else "DEGRADED"
            
            towers.append((t_id, lat, lon, circle, status))
            
            for idx, azimuth in enumerate([0, 120, 240]):
                sec_char = chr(65 + idx)
                s_id = f"{t_id}_SEC_{sec_char}"
                band = random.choice(["3500MHz_n78", "700MHz_n28"])
                sectors.append((s_id, t_id, azimuth, band))

        cursor.executemany("INSERT INTO network_infra.cell_towers VALUES (%s, %s, %s, %s, %s);", towers)
        cursor.executemany("INSERT INTO network_infra.tower_sectors VALUES (%s, %s, %s, %s);", sectors)
        print("Infrastructure base assets successfully written.")
    else:
        print(f"Existing infrastructure anchors discovered ({existing_towers} towers). Skipping hardware duplicate writes.")

    # -- 2. GENERATE COMPREHENSIVE SUBSCRIBER PROFILES --
    subscribers, kyc_records, analytics, sim_cards, devices, billing_accts, ledgers, allowances = ([] for _ in range(8))

    for _ in range(num_subscribers):
        sub_id = str(uuid.uuid4())
        circle = random.choice(CIRCLES)
        
        first_name = fake.first_name()
        last_name = fake.last_name()
        phone = generate_indian_phone(circle)
        email = f"{first_name.lower()}.{last_name.lower()}{random.randint(10,9999)}@{fake.free_email_domain()}"
        
        # FIXED: Enforce strictly LIVE or trailing sub-hour timestamps to satisfy modern streaming watermarks
        if not APPEND_MODE:
            # Baseline spread within the last 1-2 hours for safe processing sequence
            create_ts = current_live_time - timedelta(minutes=random.randint(5, 120))
        else:
            # Incremental updates occur exactly RIGHT NOW to ensure they beat the watermark ceiling
            create_ts = datetime.now(timezone.utc)
        
        subscribers.append((sub_id, first_name, last_name, phone, email, circle, create_ts, create_ts))
        
        # Child 1: KYC Profile
        kyc_id = str(uuid.uuid4())
        pan = generate_indian_pan(last_name)
        raw_aadhaar = generate_raw_aadhaar()
        kyc_records.append((kyc_id, sub_id, raw_aadhaar, pan, 'VERIFIED', create_ts))
        
        # Child 2: Analytics segmentations
        clv = random.randint(49900, 2500000)
        churn = round(random.uniform(0.00, 0.85), 2)
        tier = random.choices(["GOLD", "SILVER", "BRONZE"], weights=[0.1, 0.25, 0.65], k=1)[0]
        analytics.append((sub_id, clv, churn, tier))
        
        # Child 3: Physical SIM card allocation
        sim_cards.append((str(uuid.uuid4()), sub_id, generate_imsi(), generate_iccid(), 'ACTIVE', create_ts))
        
        # Child 4: Linked Hardware device tracking
        devices.append((str(uuid.uuid4()), sub_id, generate_imei(), True, random.choice(OS_VARIANTS), create_ts))
        
        # Child 5: Financial Accounts configuration
        acct_id = str(uuid.uuid4())
        b_type = random.choice(["PREPAID", "POSTPAID"])
        bal = random.randint(0, 150000) if b_type == "PREPAID" else random.randint(-50000, 0)
        billing_accts.append((acct_id, sub_id, b_type, bal, create_ts, create_ts))
        
        # Child 6: Active Allowance tracking
        plan = random.choice(PLAN_CODES)
        quota = PLAN_QUOTAS[plan]
        consumed = int(quota * random.uniform(0.05, 0.92))
        allowances.append((str(uuid.uuid4()), acct_id, plan, quota, consumed, create_ts))
        
        # Child 7: Transaction History generation entries
        if random.random() > 0.1:
            txn_id = str(uuid.uuid4())
            tx_amount = random.choice([29900, 47900, 74900, 99900])
            pay_method = random.choice(["UPI", "CREDIT_CARD", "NETBANKING"])
            gateway = random.choice(["RAZORPAY", "CASHFREE"])
            # Transaction timestamps hit strictly at current system runtime execution second
            txn_ts = datetime.now(timezone.utc)
            ledgers.append((txn_id, acct_id, tx_amount, pay_method, gateway, txn_ts))

    print("Writing transaction arrays to cluster engines...")
    
    try:
        cursor.executemany("INSERT INTO crm.subscribers VALUES (%s, %s, %s, %s, %s, %s, %s, %s);", subscribers)
        cursor.executemany("INSERT INTO crm.subscriber_kyc VALUES (%s, %s, %s, %s, %s, %s);", kyc_records)
        cursor.executemany("INSERT INTO crm.customer_analytics_segments VALUES (%s, %s, %s, %s);", analytics)
        cursor.executemany("INSERT INTO inventory.sim_cards VALUES (%s, %s, %s, %s, %s, %s);", sim_cards)
        cursor.executemany("INSERT INTO inventory.device_registry VALUES (%s, %s, %s, %s, %s, %s);", devices)
        cursor.executemany("INSERT INTO billing.accounts VALUES (%s, %s, %s, %s, %s, %s);", billing_accts)
        cursor.executemany("INSERT INTO billing.data_allowance_ledgers VALUES (%s, %s, %s, %s, %s, %s);", allowances)
        if ledgers:
            cursor.executemany("INSERT INTO billing.payment_ledger VALUES (%s, %s, %s, %s, %s, %s);", ledgers)
        print("Data loading cycles finalized successfully.")
    except Exception as e:
        print(f"Batch execution conflict detected: {e}. Rolling back current database cycle transaction block.")
        raise e

# ----------------------------------------------------------------------
# 6. ENGINE MAIN COORDINATOR RUNNER
# ----------------------------------------------------------------------
if __name__ == "__main__":
    print("Establishing connection to core PostgreSQL cluster...")
    try:
        if not os.getenv("DB_NAME"):
            raise ValueError("Environment configurations missing. Please verify your local .env context file.")

        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False  # Handle manually via transactional commits
        cur = conn.cursor()
        
        # Execute Setup Definitions
        setup_database_schema(cur)
        
        # Populate Tables
        populate_production_data(cur)
        
        # Commit transaction cleanly
        conn.commit()
        print("\n=== TRACK 1 SYSTEM INITIALIZATION SUCCESSFUL ===")
        print(f"Mode Applied: {'APPEND / INCREMENTAL' if APPEND_MODE else 'CLEAN RESET'} . Verification row sets committed.")
        
    except (Exception, psycopg2.DatabaseError) as ex:
        if 'conn' in locals() and conn:
            conn.rollback()
        print(f"\nFATAL DATABASE INITIALIZATION EXCEPTION TRIGGERED:\n{ex}")
    finally:
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()
        print("Database connection closed cleanly.")