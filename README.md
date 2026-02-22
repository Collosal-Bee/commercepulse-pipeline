# CommercePulse Analytics Pipeline

## Architecture Overview
This project implements a hybrid, dual-store data architecture to reconcile CommercePulse's legacy historical data dumps with its modern, high-velocity live event streams. 

### Core Assumptions
1. **Currency Standardization:** It is assumed for this iteration that historical amounts are recorded as-is. A future enhancement would join the `fx_rates_2023.csv` to normalize NGN/USD into a single reporting currency.
2. **BigQuery Auto-Schema:** While BigQuery infers the schema directly from the Pandas DataFrames via the Python client, explicit DDL statements (including Partitioning and Clustering strategies) are documented in `sql/schemas.sql`.
3. **Incremental Extraction via Truncation:** For this scale, the pipeline achieves idempotency by utilizing `WRITE_TRUNCATE` in BigQuery. For massive scale, a true incremental load utilizing a watermarking table (tracking `last_ingested_at`) would be implemented.

### Trade-Off Analysis & Engineering Decisions

**1. MongoDB vs BigQuery Responsibilities**
* **Decision:** MongoDB acts exclusively as the Raw Event Landing Zone (System of Record), while BigQuery serves as the structured Analytics Warehouse.
* **Trade-off:** Querying raw JSON directly for analytics is slow and expensive. By decoupling storage (MongoDB) from compute/analytics (BigQuery), we preserve the untouched raw payloads for engineering audits while providing business analysts with lightning-fast, highly structured SQL tables. 

**2. Historical Batch vs Live Event Handling**
* **Decision:** We unified both paradigms into a single "Event-Driven" model. Historical batch records were algorithmically wrapped into synthetic events with deterministic IDs. 
* **Trade-off:** Instead of building two separate pipelines, standardizing everything into an event model simplified the ingestion layer, ensuring live events and historical data coexist seamlessly.

**3. Append-Only vs Upsert Strategies**
* **Decision:** In MongoDB, we utilized an `upsert` strategy anchored on deterministic `event_id` hashes. 
* **Trade-off:** Live events often arrive duplicated or out-of-order. Upserting in MongoDB guarantees deduplication at the source without complex locking mechanisms, ensuring the pipeline can be re-run safely without bloating the database.

**4. Pandas vs SQL Transformations**
* **Decision:** We utilized Pandas as an in-memory reconciliation layer prior to BigQuery insertion.
* **Trade-off:** Dealing with severe schema drift (e.g., `order_id` vs `orderRef` vs nested `{"order": {"id": ...}}`) is notoriously brittle in pure SQL. Pandas allows for robust, dynamic dictionary unpacking, ensuring clean enforcement of the schema before it hits the warehouse.

## How to Run
1. `python src/ingest_historical.py` (Loads legacy data to Mongo)
2. `python src/ingest_live.py` (Catches new live streams)
3. `python src/transform_load.py` (Normalizes schemas and loads to BigQuery)
4. `python src/data_quality.py` (Generates daily anomaly report)

## Data Quality Verification
![Daily DQ Report](dq_report.png)