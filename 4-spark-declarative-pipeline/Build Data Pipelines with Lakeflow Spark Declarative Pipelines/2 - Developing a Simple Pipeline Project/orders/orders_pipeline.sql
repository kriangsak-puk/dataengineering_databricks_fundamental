----------------------------
-- ORDERS - LAKEFLOW SPARK DECLARATIVE PIPELINE
----------------------------
-------------------------------------------------------
-- SQL Pipeline Code
-------------------------------------------------------
-- NOTE: The default catalog is set to your 'workspace' catalog, so specifying the catalog is required for the code below since we are using the pipeline catalog.
-------------------------------------------------------

-- A. Create the bronze streaming table in your workspace.1_bronze_db schema from a JSON files in your volume
  -- NOTE: read_files references the 'source' configuration key from your pipeline settings. 
  -- NOTE: 'source' = '/Volumes/pipeline/pipeline_data/data'
CREATE OR REFRESH STREAMING TABLE 1_bronze_db.orders_bronze_demo2 
AS 
SELECT 
  *,
  current_timestamp() AS processing_time,
  _metadata.file_name AS source_file
FROM STREAM read_files(  -- Performs incremental ingestion with checkpoints using Auto Loader
    "${source}/orders",  -- Uses the source configuration variable set in the pipeline settings
    format => 'JSON'
);


-- B. Create the silver streaming table in your workspace.2_silver_db schema (database)
CREATE OR REFRESH STREAMING TABLE 2_silver_db.orders_silver_demo2 
AS 
SELECT 
  order_id,
  timestamp(order_timestamp) AS order_timestamp, 
  customer_id,
  notifications
FROM STREAM 1_bronze_db.orders_bronze_demo2 ; -- References the streaming orders_bronze table for incrementally processing


-- C. Create the materialized view aggregation from the orders_silver table with the summarization
CREATE OR REFRESH MATERIALIZED VIEW 3_gold_db.gold_orders_by_date_demo2 
AS 
SELECT 
  date(order_timestamp) AS order_date, 
  count(*) AS total_daily_orders
FROM 2_silver_db.orders_silver_demo2  -- Aggregates the full orders_silver streaming table with optimizations where applicable
GROUP BY date(order_timestamp);
---------------------------------------------------------
