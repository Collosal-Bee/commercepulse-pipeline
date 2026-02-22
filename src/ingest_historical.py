import json
import hashlib
import os
from datetime import datetime, timezone
from pymongo import MongoClient
from dotenv import load_dotenv

# 1. Load our environment variables (your MongoDB connection string)
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "commercepulse")

# 2. Connect to the MongoDB database and specifically the 'events_raw' collection
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
collection = db["events_raw"]

def generate_event_id(record_string):
    """
    Creates a unique, deterministic hash (barcode) for every record.
    If we feed it the exact same record twice, it generates the exact same ID.
    """
    return hashlib.sha256(record_string.encode('utf-8')).hexdigest()

def load_historical_file(filepath, event_type):
    print(f"Loading {filepath}...")
    
    # Open and read the JSON file
    with open(filepath, 'r') as file:
        data = json.load(file)
        
    ingested_at = datetime.now(timezone.utc).isoformat()
    
    for record in data:
        # Turn the raw JSON record into a string so we can hash it
        record_str = json.dumps(record, sort_keys=True)
        event_id = generate_event_id(record_str)
        
        # Historical data is incredibly messy. We try to grab a timestamp if it exists, 
        # otherwise we fall back to the time we ingested it.
        event_time = record.get("created_at") or record.get("paid_at") or record.get("paidAt") or record.get("refundedAt") or ingested_at
        
        # 3. WRAP THE EVENT: We place the original raw data inside a standard "envelope"
        document = {
            "event_id": event_id,
            "event_type": event_type,
            "event_time": str(event_time),
            "vendor": "historical_legacy", # We tag it so we know it's from the old system
            "payload": record,             # The untouched, original raw data
            "ingested_at": ingested_at
        }
        
        # 4. IDEMPOTENT INSERT (UPSERT)
        # We tell MongoDB: "Find a document with this event_id. If it exists, update it. If not, insert it."
        # This guarantees we NEVER get duplicate rows, even if you run this script 100 times.
        collection.update_one({"event_id": event_id}, {"$set": document}, upsert=True)
        
    print(f"✅ Finished loading {event_type}. Total records processed: {len(data)}")

if __name__ == "__main__":
    # Point the script to the folder where you extracted the historical data
    base_dir = "data/bootstrap"
    
    # Process all four files
    load_historical_file(f"{base_dir}/orders_2023.json", "historical_order")
    load_historical_file(f"{base_dir}/payments_2023.json", "historical_payment")
    load_historical_file(f"{base_dir}/shipments_2023.json", "historical_shipment")
    load_historical_file(f"{base_dir}/refunds_2023.json", "historical_refund")
    
    print("🚀 Historical bootstrap successfully completed!")