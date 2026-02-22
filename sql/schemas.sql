
-- 1. FACT_ORDERS (Current State)
CREATE TABLE IF NOT EXISTS `analytics_db.fact_orders` (
    order_id STRING OPTIONS(description="Standardized unique order identifier"),
    event_time TIMESTAMP OPTIONS(description="Time the order was created or last updated"),
    amount FLOAT64 OPTIONS(description="Total order value standardized to numeric"),
    vendor STRING OPTIONS(description="Source system or legacy indicator")
)
PARTITION BY DATE(event_time)
CLUSTER BY vendor, order_id;

-- 2. FACT_PAYMENTS (Append-Only)
CREATE TABLE IF NOT EXISTS `analytics_db.fact_payments` (
    event_id STRING OPTIONS(description="Deterministic hash of the raw payload"),
    order_id STRING OPTIONS(description="Foreign key to fact_orders"),
    event_time TIMESTAMP OPTIONS(description="Time of payment attempt"),
    amount FLOAT64 OPTIONS(description="Amount paid"),
    status STRING OPTIONS(description="Standardized payment state (SUCCESS, FAILED, etc.)"),
    vendor STRING OPTIONS(description="Source system")
)
PARTITION BY DATE(event_time)
CLUSTER BY status, vendor;

-- 3. FACT_REFUNDS (Append-Only)
CREATE TABLE IF NOT EXISTS `analytics_db.fact_refunds` (
    event_id STRING OPTIONS(description="Deterministic hash of the raw payload"),
    order_id STRING OPTIONS(description="Foreign key to fact_orders"),
    event_time TIMESTAMP OPTIONS(description="Time of refund"),
    amount FLOAT64 OPTIONS(description="Amount refunded"),
    reason STRING OPTIONS(description="Standardized refund reason"),
    vendor STRING OPTIONS(description="Source system")
)
PARTITION BY DATE(event_time)
CLUSTER BY reason, vendor;

-- 4. FACT_SHIPMENTS
CREATE TABLE IF NOT EXISTS `analytics_db.fact_shipments` (
    event_id STRING,
    order_id STRING,
    event_time TIMESTAMP,
    carrier STRING,
    status STRING
)
PARTITION BY DATE(event_time);

-- 5. DIM_DATE (Auto-generated calendar dimension)
CREATE OR REPLACE TABLE `analytics_db.dim_date` AS
SELECT
  CAST(d AS DATE) AS date_id,
  EXTRACT(YEAR FROM d) AS year,
  EXTRACT(MONTH FROM d) AS month,
  EXTRACT(DAY FROM d) AS day
FROM UNNEST(GENERATE_TIMESTAMP_ARRAY('2023-01-01 00:00:00', '2026-12-31 00:00:00', INTERVAL 1 DAY)) AS d;

-- 6. FACT_ORDER_DAILY (Derived Aggregate Table for fast BI querying)
CREATE OR REPLACE TABLE `analytics_db.fact_order_daily` AS
SELECT 
  DATE(event_time) AS order_date,
  vendor,
  COUNT(DISTINCT order_id) AS total_orders,
  SUM(amount) AS gross_revenue
FROM `analytics_db.fact_orders`
GROUP BY 1, 2;