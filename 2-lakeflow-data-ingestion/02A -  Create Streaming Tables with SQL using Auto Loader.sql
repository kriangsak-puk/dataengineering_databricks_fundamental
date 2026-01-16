-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img
-- MAGIC     src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png"
-- MAGIC     alt="Databricks Learning"
-- MAGIC   >
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 2B -  Create Streaming Tables with SQL using Auto Loader
-- MAGIC
-- MAGIC In this demonstration we will create a streaming table to incrementally ingest files from a volume using Auto Loader with SQL. 
-- MAGIC
-- MAGIC When you create a streaming table using the CREATE OR REFRESH STREAMING TABLE statement, the initial data refresh and population begin immediately. These operations do not consume DBSQL warehouse compute. Instead, streaming table rely on serverless DLT for both creation and refresh. A dedicated serverless DLT pipeline is automatically created and managed by the system for each streaming table.
-- MAGIC
-- MAGIC ### Learning Objectives
-- MAGIC
-- MAGIC By the end of this lesson, you should be able to:
-- MAGIC - Create streaming tables in Databricks SQL for incremental data ingestion.
-- MAGIC - Refresh streaming tables using the REFRESH statement.
-- MAGIC
-- MAGIC ### RECOMMENDATION
-- MAGIC
-- MAGIC The CREATE STREAMING TABLE SQL command is the recommended alternative to the legacy COPY INTO SQL command for incremental ingestion from cloud object storage. Databricks recommends using streaming tables to ingest data using Databricks SQL. 
-- MAGIC
-- MAGIC A streaming table is a table registered to Unity Catalog with extra support for streaming or incremental data processing. A DLT pipeline is automatically created for each streaming table. You can use streaming tables for incremental data loading from Kafka and cloud object storage.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Lab setup

-- COMMAND ----------

-- MAGIC %run ./_resources/00-setup $reset_all_data=false

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## REQUIRED - SELECT YOUR SERVERLESS SQL WAREHOUSE
-- MAGIC
-- MAGIC **NOTE: Creating streaming tables with Databricks SQL requires a SQL warehouse.**.
-- MAGIC
-- MAGIC <!-- ![Select Cluster](./Includes/images/selecting_cluster_info.png) -->
-- MAGIC
-- MAGIC Before executing cells in this notebook, please select the **SHARED SQL WAREHOUSE** in the lab. Follow these steps:
-- MAGIC
-- MAGIC 1. Navigate to the top-right of this notebook and click the drop-down to select compute (it might say **Connect**). Complete one of the following below:
-- MAGIC
-- MAGIC    a. Under **Recent resources**, check to see if you have a **shared_warehouse SQL**. If you do, select it.
-- MAGIC
-- MAGIC    b. If you do not have a **shared_warehouse** under **Recent resources**, complete the following:
-- MAGIC
-- MAGIC     - In the same drop-down, select **More**.
-- MAGIC
-- MAGIC     - Then select the **SQL Warehouse** button.
-- MAGIC
-- MAGIC     - In the drop-down, make sure **shared_warehouse** is selected.
-- MAGIC
-- MAGIC     - Then, at the bottom of the pop-up, select **Start and attach**.
-- MAGIC
-- MAGIC </br>
-- MAGIC    <img src="./Includes/images/sql_warehouse_xsmall.png" alt="SQL Warehouse" width="600">

-- COMMAND ----------

-- DBTITLE 1,Check your default catalog and schema
SELECT current_catalog(), current_schema()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## B. Create Streaming Tables for Incremental Processing

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Run the query below to view the data in the CSV file(s) in your cloud storage location. Notice that it was returned in tabular format.

-- COMMAND ----------

-- DBTITLE 1,Preview the CSV file in your volume

SELECT *
FROM read_files(
  '/Volumes/main/dbdemos_data_ingestion/raw_data/user_csv_pipe_delimited',
  format => 'CSV',
  sep => '|',
  header => true
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Create a STREAMING TABLE using Databricks SQL
-- MAGIC 3. Your goal is to create an incremental pipeline that only ingests new files (instead of using traditional batch ingestion). You can achieve this by using [streaming tables in Databricks SQL](https://docs.databricks.com/aws/en/dlt/dbsql/streaming) (Auto Loader).
-- MAGIC
-- MAGIC    - The SQL code below creates a streaming table that will be scheduled to incrementally ingest only new data every week. 
-- MAGIC    
-- MAGIC    - A pipeline is automatically created for each streaming table. You can use streaming tables for incremental data loading from Kafka and cloud object storage.
-- MAGIC
-- MAGIC    **NOTE:** Incremental batch ingestion automatically detects new records in the data source and ignores records that have already been ingested. This reduces the amount of data processed, making ingestion jobs faster and more efficient in their use of compute resources.
-- MAGIC
-- MAGIC    **REQUIRED: Please insert the path of your csv_files_autoloader_source volume in the `read_files` function. This process will take about a minute to run and set up the incremental ingestion pipeline.**

-- COMMAND ----------

-- DBTITLE 1,Create a streaming table

CREATE OR REFRESH STREAMING TABLE sql_csv_autoloader
SCHEDULE EVERY 1 WEEK     -- Scheduling the refresh is optional
AS
SELECT *
FROM STREAM read_files(
  '/Volumes/main/dbdemos_data_ingestion/raw_data/user_csv_pipe_delimited',  -- Insert the path to CSV
  format => 'CSV',
  sep => '|',
  header => true
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Complete the following to view the streaming table in your catalog.
-- MAGIC
-- MAGIC    a. Select the catalog icon on the left ![Catalog Icon](./Includes/images/catalog_icon.png).
-- MAGIC
-- MAGIC    b. Expand the **workspace** catalog.
-- MAGIC
-- MAGIC    c. Expand your **default** schema.
-- MAGIC
-- MAGIC    d. Expand your **Tables**.
-- MAGIC
-- MAGIC    e. Find the **sql_csv_autoloader** table. Notice that the Delta streaming table icon is slightly different from a traditional Delta table:
-- MAGIC     
-- MAGIC     ![Streaming table icon](./Includes/images/streaming_table_icon.png) 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5. Run the cell below to view the streaming table. Confirm that the results contain **5000
-- MAGIC  rows**.

-- COMMAND ----------

-- DBTITLE 1,View the streaming table
SELECT *
FROM sql_csv_autoloader;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6. Describe the STREAMING TABLE and view the results. Notice the following:
-- MAGIC
-- MAGIC - Under **Detailed Table Information**, notice the following rows:
-- MAGIC   - **View Text**: The query that created the table.
-- MAGIC   - **Type**: Specifies that it is a STREAMING TABLE.
-- MAGIC   - **Provider**: Indicates that it is a Delta table.
-- MAGIC
-- MAGIC - Under **Refresh Information**, you can see specific refresh details. Example shown below:
-- MAGIC
-- MAGIC ##### Refresh Information
-- MAGIC
-- MAGIC | Field                   | Value                                                                                                                                         |
-- MAGIC |-------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
-- MAGIC | Last Refreshed          | 2025-06-17T16:12:49.168Z                                                                                                                      |
-- MAGIC | Last Refresh Type       | INCREMENTAL                                                                                                                                   |
-- MAGIC | Latest Refresh Status   | Succeeded                                                                                                                                     |
-- MAGIC | Latest Refresh          | https://example.url.databricks.com/#joblist/pipelines/bed6c715-a7c1-4d45-b57c-4fdac9f956a7/updates/9455a2ef-648c-4339-b61e-d282fa76a92c (this is the path to the Declarative Pipeline that was created for you)|
-- MAGIC | Refresh Schedule        | EVERY 1 WEEK                                                                                                                                 |

-- COMMAND ----------

-- DBTITLE 1,View the metadata of the streaming table
DESCRIBE TABLE EXTENDED sql_csv_autoloader;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 7. The `DESCRIBE HISTORY` statement displays a detailed list of all changes, versions, and metadata associated with a Delta streaming table, including information on updates, deletions, and schema changes.
-- MAGIC
-- MAGIC     Run the cell below and view the results. Notice the following:
-- MAGIC
-- MAGIC     - In the **operation** column, you can see that a streaming table performs three operations: **CREATE TABLE**, **DLT SETUP** and **STREAMING UPDATE**.
-- MAGIC     
-- MAGIC     - Scroll to the right and find the **operationMetrics** column. In row 1 (Version 2 of the table), the value shows that the **numOutputRows** is 10,000, indicating that  10,000 rows were added to the **sql_csv_autoloader** table.

-- COMMAND ----------

-- DBTITLE 1,View the history of the streaming table
DESCRIBE HISTORY sql_csv_autoloader;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 8. Complete the following steps to manually add another file to your cloud storage location:  
-- MAGIC    `/Volumes/main/dbdemos_data_ingestion/raw_data/user_csv_pipe_delimited/`.
-- MAGIC
-- MAGIC    a. go to workspace file, download file name 02A-test-file.csv
-- MAGIC
-- MAGIC    b. Upload the downloaded **02A-test-file.csv** file to the **user_csv_pipe_delimited** volume:
-- MAGIC
-- MAGIC       - Right-click on the **user_csv_pipe_delimited** volume. 
-- MAGIC
-- MAGIC       - Select **Upload to volume**.  
-- MAGIC
-- MAGIC       - Choose and upload the **02A-test-file.csv** file from your local machine.
-- MAGIC
-- MAGIC    c. Confirm your volume **user_csv_pipe_delimited** contains new CSV file.
-- MAGIC
-- MAGIC
-- MAGIC     **NOTE:** Depending on your laptop’s security settings, you may not be able to download files locally.|
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 9. Next, manually refresh the STREAMING TABLE using `REFRESH STREAMING TABLE table-name`. 
-- MAGIC
-- MAGIC - [Refresh a streaming table](https://docs.databricks.com/aws/en/dlt/dbsql/streaming#refresh-a-streaming-table) documentation
-- MAGIC
-- MAGIC     **NOTE:** You can also go back to **Create a STREAMING TABLE using Databricks SQL (direction number 3)** and rerun that cell to incrementally ingest only new files. Once complete come back to step 8.

-- COMMAND ----------

REFRESH STREAMING TABLE sql_csv_autoloader;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 10. Run the cell below to view the data in the **sql_csv_autoloader** table. Notice that the table now contains **5000 rows**.
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,View the streaming table
SELECT *
FROM sql_csv_autoloader;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 11. Describe the history of the **sql_csv_autoloader** table. Observe the following:
-- MAGIC
-- MAGIC   - Version 3 of the streaming table includes another **STREAMING UPDATE**.
-- MAGIC
-- MAGIC   - Expand the **operationMetrics** column and note that only **5003 rows** were incrementally ingested into the table from the new **02A-test-file** file.
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,View the history of the streaming table
DESCRIBE HISTORY sql_csv_autoloader;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 12. Drop the streaming table.

-- COMMAND ----------

-- DBTITLE 1,Drop the streaming table
DROP TABLE IF EXISTS sql_csv_autoloader;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Additional Resources
-- MAGIC
-- MAGIC - [Streaming Tables Documentation](https://docs.databricks.com/gcp/en/dlt/streaming-tables)
-- MAGIC
-- MAGIC - [CREATE STREAMING TABLE Syntax](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-streaming-table)
-- MAGIC
-- MAGIC - [Using Streaming Tables in Databricks SQL](https://docs.databricks.com/aws/en/dlt/dbsql/streaming)
-- MAGIC
-- MAGIC - [REFRESH (MATERIALIZED VIEW or STREAMING TABLE)](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-refresh-full)
-- MAGIC
-- MAGIC - [COPY INTO (legacy)](https://docs.databricks.com/aws/en/ingestion/#copy-into-legacy)
-- MAGIC
-- MAGIC - [Lakeflow Declarative Pipelines](https://docs.databricks.com/aws/en/dlt/)
-- MAGIC ---
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
