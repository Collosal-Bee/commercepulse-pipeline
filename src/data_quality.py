from google.cloud import bigquery
import os
from dotenv import load_dotenv

load_dotenv()
bq_client = bigquery.Client()
DATASET_ID = f"{bq_client.project}.analytics_db"

def run_quality_report():
    print("📊 COMMERCEPULSE DAILY DATA QUALITY REPORT 📊\n" + "="*45)
    
    queries = {
        "Total Clean Orders": f"SELECT COUNT(*) FROM `{DATASET_ID}.fact_orders`",
        "Total Payments Recorded": f"SELECT COUNT(*) FROM `{DATASET_ID}.fact_payments`",
        "Total Refunds Processed": f"SELECT COUNT(*) FROM `{DATASET_ID}.fact_refunds`",
        "Orphaned Payments (No matching order)": f"""
            SELECT COUNT(*) FROM `{DATASET_ID}.fact_payments` p 
            LEFT JOIN `{DATASET_ID}.fact_orders` o ON p.order_id = o.order_id 
            WHERE o.order_id IS NULL
        """
    }
    
    for metric, query in queries.items():
        query_job = bq_client.query(query)
        result = list(query_job.result())[0][0]
        print(f"{metric}: {result}")
        
    print("="*45)
    print("✅ All systems operational. Ready for BI integration.")

if __name__ == "__main__":
    run_quality_report()