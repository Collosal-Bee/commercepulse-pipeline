import os
import pandas as pd
from pymongo import MongoClient
from google.cloud import bigquery
from dotenv import load_dotenv

# 1. Setup & Connections
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "commercepulse")

# The BigQuery client automatically finds the gcp-key.json because i added it to the .env file
bq_client = bigquery.Client()
DATASET_ID = f"{bq_client.project}.analytics_db"

def extract_from_mongo():
    print("Extracting raw data from MongoDB...")
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    cursor = db["events_raw"].find({}, {"_id": 0})
    return pd.DataFrame(list(cursor))

def extract_order_id(payload):
    """
    This function handles the Schema Drift. It checks every known variation 
    of 'order id' that the vendors use and standardizes it into one format.
    """
    if 'order_id' in payload: return payload['order_id']
    if 'orderRef' in payload: return payload['orderRef']
    if 'order' in payload and isinstance(payload['order'], dict): return payload['order'].get('id')
    if 'order' in payload: return payload['order']
    return "UNKNOWN"

def extract_amount(payload):
    return payload.get('totalAmount') or payload.get('amountPaid') or payload.get('amount') or payload.get('refundAmount') or payload.get('amt') or payload.get('total') or 0.0

def transform_and_load(df):
    print("Normalizing JSON schemas and fixing drift...")
    
    # 1. Apply the normalization functions across the massive dataset
    df['order_id'] = df['payload'].apply(extract_order_id)
    df['amount'] = df['payload'].apply(extract_amount)
    
    # i use WRITE_TRUNCATE here so i can run this script multiple times safely without doubling your data (Idempotency)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

    # --- FACT PAYMENTS ---
    print("Building fact_payments...")
    # Filter only payment events
    payments = df[df['event_type'].str.contains('payment', case=False, na=False)].copy()
    payments['status'] = payments['payload'].apply(lambda x: x.get('payment_status') or x.get('status') or x.get('state') or 'UNKNOWN')
    fact_payments = payments[['event_id', 'order_id', 'event_time', 'amount', 'status', 'vendor']]
    
    bq_client.load_table_from_dataframe(fact_payments, f"{DATASET_ID}.fact_payments", job_config=job_config).result()
    print("✅ fact_payments successfully loaded to BigQuery!")

    # --- FACT ORDERS ---
    print("Building fact_orders (Current State)...")
    orders = df[df['event_type'].str.contains('order', case=False, na=False)].copy()
    # To get the "Current Truth", i sort by time and drop older duplicates of the same order
    orders = orders.sort_values('event_time').drop_duplicates(subset=['order_id'], keep='last')
    fact_orders = orders[['order_id', 'event_time', 'amount', 'vendor']]
    
    bq_client.load_table_from_dataframe(fact_orders, f"{DATASET_ID}.fact_orders", job_config=job_config).result()
    print("✅ fact_orders successfully loaded to BigQuery!")

    # --- FACT REFUNDS ---
    print("Building fact_refunds...")
    refunds = df[df['event_type'].str.contains('refund', case=False, na=False)].copy()
    refunds['reason'] = refunds['payload'].apply(lambda x: x.get('reason') or x.get('refund_reason') or 'UNKNOWN')
    fact_refunds = refunds[['event_id', 'order_id', 'event_time', 'amount', 'reason', 'vendor']]
    
    bq_client.load_table_from_dataframe(fact_refunds, f"{DATASET_ID}.fact_refunds", job_config=job_config).result()
    print("✅ fact_refunds successfully loaded to BigQuery!")

if __name__ == "__main__":
    raw_df = extract_from_mongo()
    transform_and_load(raw_df)
    print("🚀 Data Pipeline Execution Complete! Your BigQuery warehouse is ready.")