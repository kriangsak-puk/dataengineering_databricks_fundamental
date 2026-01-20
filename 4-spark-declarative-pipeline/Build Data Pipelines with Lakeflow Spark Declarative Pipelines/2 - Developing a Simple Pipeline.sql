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
-- MAGIC # 2 - Developing a Simple Pipeline
-- MAGIC
-- MAGIC In this demonstration, we will create a simple Lakeflow Spark Declarative Pipeline project using the new **Lakeflow Pipeline Editor** with declarative SQL.
-- MAGIC
-- MAGIC
-- MAGIC ### Learning Objectives
-- MAGIC
-- MAGIC By the end of this lesson, you will be able to:
-- MAGIC - Describe the SQL syntax used to create a Lakeflow Spark Declarative Pipeline.
-- MAGIC - Navigate the Lakeflow Pipeline Editor to modify pipeline settings and ingest the raw data source file(s).
-- MAGIC - Create, execute and monitor a Spark Declarative Pipeline.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Setup
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def copy_files(copy_from: str, copy_to: str, n: int, sleep=2):
-- MAGIC     '''
-- MAGIC     Copy files from one location to another destination's volume.
-- MAGIC
-- MAGIC     This method performs the following tasks:
-- MAGIC       1. Lists files in the source directory and sorts them. Sorted to keep them in the same order when copying for consistency.
-- MAGIC       2. Verifies that the source directory has at least `n` files.
-- MAGIC       3. Copies files from the source to the destination, skipping files already present at the destination.
-- MAGIC       4. Pauses for `sleep` seconds after copying each file.
-- MAGIC       5. Stops after copying `n` files or if all files are processed.
-- MAGIC       6. Will print information on the files copied.
-- MAGIC     
-- MAGIC     Parameters
-- MAGIC     - copy_from (str): The source directory where files are to be copied from.
-- MAGIC     - copy_to (str): The destination directory where files will be copied to.
-- MAGIC     - n (int): The number of files to copy from the source. If n is larger than total files, an error is returned.
-- MAGIC     - sleep (int, optional): The number of seconds to pause after copying each file. Default is 2 seconds.
-- MAGIC
-- MAGIC     Returns:
-- MAGIC     - None: Prints information to the log on what files it's loading. If the file exists, it skips that file.
-- MAGIC
-- MAGIC     Example:
-- MAGIC     - copy_files(copy_from='/Volumes/gym_data/v01/user-reg', 
-- MAGIC            copy_to=f'{DA.paths.working_dir}/pii/stream_source/user_reg',
-- MAGIC            n=1)
-- MAGIC     '''
-- MAGIC     import os
-- MAGIC     import time
-- MAGIC
-- MAGIC     print(f"\n----------------Loading files to user's volume: '{copy_to}'----------------")
-- MAGIC
-- MAGIC     ## List all files in the copy_from volume and sort the list
-- MAGIC     list_of_files_to_copy = sorted(os.listdir(copy_from))
-- MAGIC     total_files_in_copy_location = len(list_of_files_to_copy)
-- MAGIC
-- MAGIC     ## Get a list of files in the source
-- MAGIC     list_of_files_in_source = os.listdir(copy_to)
-- MAGIC
-- MAGIC     assert total_files_in_copy_location >= n, f"The source location contains only {total_files_in_copy_location} files, but you specified {n}  files to copy. Please specify a number less than or equal to the total number of files available."
-- MAGIC
-- MAGIC     ## Looping counter
-- MAGIC     counter = 1
-- MAGIC
-- MAGIC     ## Load files if not found in the co
-- MAGIC     for file in list_of_files_to_copy:
-- MAGIC
-- MAGIC       ## If the file is found in the source, skip it with a note. Otherwise, copy file.
-- MAGIC       if file in list_of_files_in_source:
-- MAGIC         print(f'File number {counter} - {file} is already in the source volume "{copy_to}". Skipping file.')
-- MAGIC       else:
-- MAGIC         file_to_copy = f'{copy_from}/{file}'
-- MAGIC         copy_file_to = f'{copy_to}/{file}'
-- MAGIC         print(f'File number {counter} - Copying file {file_to_copy} --> {copy_file_to}.')
-- MAGIC         dbutils.fs.cp(file_to_copy, copy_file_to , recurse = True)
-- MAGIC         
-- MAGIC         ## Sleep after load
-- MAGIC         time.sleep(sleep) 
-- MAGIC
-- MAGIC       ## Stop after n number of loops based on argument.
-- MAGIC       if counter == n:
-- MAGIC         break
-- MAGIC       else:
-- MAGIC         counter = counter + 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## B. Developing and Running a Spark Declarative Pipeline with the Lakeflow Pipeline Editor
-- MAGIC
-- MAGIC This course includes a simple, pre-configured Spark Declarative Pipeline to explore and modify. 
-- MAGIC
-- MAGIC In this section, we will:
-- MAGIC
-- MAGIC - Explore the Lakeflow Pipeline Editor and the declarative SQL syntax  
-- MAGIC - Modify pipeline settings  
-- MAGIC - Run the Spark Declarative Pipeline and explore the streaming tables and materialized view.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Run the cell below and **copy the path** from the output cell to your **/Volumes/pipeline/pipeline_data/data** volume. You will need this path when modifying your pipeline settings. 
-- MAGIC
-- MAGIC    This volume path contains the **orders**, **status** and **customer** directories, which contain the raw JSON files.
-- MAGIC
-- MAGIC    **EXAMPLE PATH**: `/Volumes/pipeline/pipeline_data/data`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC catalog_name = "pipeline"
-- MAGIC schema_name = ['pipeline_data', '1_bronze_db', '2_silver_db', '3_gold_db']
-- MAGIC volume_name = "data"
-- MAGIC data_volume_path = f"/Volumes/{catalog_name}/{schema_name[0]}/{volume_name}"
-- MAGIC working_dir = data_volume_path
-- MAGIC print(working_dir)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. In this course we have starter files for you to use in your pipeline. This demonstration uses the folder **2 - Developing a Simple Pipeline Project**. To create a pipeline and add existing assets to associate it with code files already available in your Workspace (including Git folders) complete the following:
-- MAGIC
-- MAGIC    a. For ease of use, open **Jobs & Pipelines** in a separate tab:
-- MAGIC
-- MAGIC     - On the main navigation bar, right-click on **Jobs & Pipelines** and select **Open in a New Tab**.
-- MAGIC
-- MAGIC    b. In **Jobs & Pipelines** select **Create** → **ETL Pipeline**.
-- MAGIC
-- MAGIC    c. Complete the pipeline creation page with the following:
-- MAGIC
-- MAGIC     - **Name**: `Name-your-pipeline-using-this-notebook-name-add-your-first-name` 
-- MAGIC     - **Default catalog**: Select your **pipeline** catalog  
-- MAGIC     - **Default schema**: Select your **default** schema (database)
-- MAGIC     - Notice there are a variety of options to start your pipeline.
-- MAGIC
-- MAGIC    d. In the options, select **Add existing assets**. In the popup, complete the following:
-- MAGIC
-- MAGIC     - **Pipeline root folder**: Select the **2 - Developing a Simple Pipeline Project** folder: 
-- MAGIC       - `~/build-data-pipelines-with-lakeflow-spark-declarative-pipelines-en_us-3.x.x/Build Data Pipelines with Lakeflow Spark Declarative Pipelines/2 - Developing a Simple Pipeline Project`
-- MAGIC
-- MAGIC     - **Source code paths**: Within the same root folder as above, select the **orders** folder: 
-- MAGIC       - `~/build-data-pipelines-with-lakeflow-spark-declarative-pipelines-en_us-3.x.x/Build Data Pipelines with Lakeflow Spark Declarative Pipelines/2 - Developing a Simple Pipeline Project/orders`
-- MAGIC
-- MAGIC     **NOTE:** You can select folders containing SQL and Python files to be executed as part of the pipeline, or you can provide individual file paths. The specified files will be processed when the pipeline runs.
-- MAGIC
-- MAGIC    e. Click **Add**, This will create a pipeline and associate the correct files for this demonstration.
-- MAGIC
-- MAGIC **Example**
-- MAGIC
-- MAGIC <img src="./Includes/images/demo02_existing_assets.png" alt="Setting Pipeline Assets" width="900">
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. In the new window, select the **orders_pipeline.sql** file and follow the instructions in the SQL file within the **Lakeflow Pipelines Editor**. 
-- MAGIC
-- MAGIC     Leave this notebook open as you will use it later.
-- MAGIC
-- MAGIC ![Orders File Directions](./Includes/images/demo02_select_orders_sqlfile.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C. Add a New File to Cloud Storage
-- MAGIC
-- MAGIC 1. After exploring and executing the pipeline by following the instructions in the **`orders_pipeline.sql`** file, run the cell below to add a new JSON file (**01.json**) to your volume at:  `/Volumes/pipeline/pipeline_data/data/orders`.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC copy_files('/Volumes/databricks_simulated_retail_customer_data/v01/retail-pipeline/orders/stream_json', 
-- MAGIC            f'{working_dir}/orders', 
-- MAGIC            n = 2)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Complete the following steps to view the new file in your volume:
-- MAGIC
-- MAGIC    a. Select the **Catalog** icon ![Catalog Icon](./Includes/images/catalog_icon.png) from the left navigation pane.  
-- MAGIC
-- MAGIC    b. Expand your **/Volumes/pipeline/pipeline_data/data** volume.  
-- MAGIC
-- MAGIC    c. Expand the **orders** directory. You should see two files in your volume: **00.json** and **01.json**.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Run the cell below to view the data in the new **/orders/01.json** file. Notice the following:
-- MAGIC
-- MAGIC    - The **01.json** file contains new orders.  
-- MAGIC    - The **01.json** file has 25 rows.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f'''
-- MAGIC   SELECT *
-- MAGIC   FROM json.`{working_dir}/orders/01.json`
-- MAGIC ''').display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Go back to the **orders_pipeline.sql** file and select **Run Pipeline** to execute your ETL pipeline again with the new file (Step 13).  
-- MAGIC
-- MAGIC    Watch the pipeline run and notice only 25 rows are added to the bronze and silver tables. 
-- MAGIC
-- MAGIC    This happens because the pipeline has already processed the first **00.json** file (174 rows), and it is now only reading the new **01.json** file (25 rows), appending the rows to the streaming tables, and recomputing the materialized view with the latest data.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## D. Exploring Your Streaming Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. View the new streaming tables and materialized view in your catalog. Complete the following:
-- MAGIC
-- MAGIC    a. Select the catalog icon ![Catalog Icon](./Includes/images/catalog_icon.png) in the left navigation pane.
-- MAGIC
-- MAGIC    b. Expand your **pipeline** catalog.
-- MAGIC
-- MAGIC    c. Expand the schemas **1_bronze_db**, **2_silver_db**, and **3_gold_db**. Notice that the two streaming tables and materialized view are correctly placed in your schemas.
-- MAGIC
-- MAGIC       - **pipeline.1_bronze_db.orders_bronze_demo2**
-- MAGIC
-- MAGIC       - **pipeline.2_silver_db.orders_silver_demo2**
-- MAGIC
-- MAGIC       - **pipeline.3_gold_db.orders_by_date_gold_demo2**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Run the cell below to view the data in the **pipeline.1_bronze_db.orders_bronze_demo2** table. Before you run the cell, how many rows should this streaming table have?
-- MAGIC
-- MAGIC    Notice the following:
-- MAGIC       - The table contains 199 rows (**00.json** had 174 rows, and **01.json** had 25 rows).
-- MAGIC       - In the **source_file** column you can see the exact file the rows were ingested from.
-- MAGIC       - In the **processing_time** column you can see the exact time the rows were ingested.

-- COMMAND ----------

SELECT *
FROM 1_bronze_db.orders_bronze_demo2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Complete the following steps to view the history of the **orders_bronze_demo2** streaming table:
-- MAGIC
-- MAGIC    a. Select the **Catalog** icon ![Catalog Icon](./Includes/images/catalog_icon.png) in the left navigation pane.  
-- MAGIC
-- MAGIC    b. Expand the **pipeline.1_bronze_db** schema.  
-- MAGIC
-- MAGIC    c. Click the three-dot (ellipsis) icon next to the **orders_bronze_demo2** table.  
-- MAGIC
-- MAGIC    d. Select **Open in Catalog Explorer**.  
-- MAGIC
-- MAGIC    e. In the Catalog Explorer, select the **History** tab. Notice an error is returned because viewing the history of a streaming table requires **SHARED_COMPUTE**. In our labs we use a **DEDICATED (formerly single user)** cluster.
-- MAGIC
-- MAGIC    f. Above your catalogs on the left select your compute cluster and change it to the provided **shared_warehouse**.
-- MAGIC
-- MAGIC    ![Change Compute](./Includes/images/change_compute.png)  
-- MAGIC
-- MAGIC    g. Go back and look at the last two versions of the table. Notice the following:  
-- MAGIC
-- MAGIC       - In the **Operation** column, the last two updates were **STREAMING UPDATE**.  
-- MAGIC
-- MAGIC       - Expand the **Operation Parameters** values for the last two updates. Notice both use `"outputMode": "Append"`.  
-- MAGIC
-- MAGIC       - Find the **Operation Metrics** column. Expand the values for the last two updates. Observe the following:
-- MAGIC
-- MAGIC          - It displays various metrics for the streaming update: **numRemovedFiles, numOutputRows, numOutputBytes, and numAddedFiles**.  
-- MAGIC
-- MAGIC          - In the `numOutputRows` values, 174 rows were added in the first update, and 25 rows in the second.
-- MAGIC
-- MAGIC    h. Close the Catalog Explorer.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## E. Viewing Spark Declarative Pipelines with the Pipelines UI
-- MAGIC
-- MAGIC After exploring and creating your pipeline using the **orders_pipeline.sql** file in the steps above, you can view the pipeline(s) you created in your workspace via the **Jobs and Pipelines** UI.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Complete the following steps to view the pipeline you created:
-- MAGIC
-- MAGIC    a. In the main applications navigation pane on the far left (you may need to expand it by selecting the ![Expand Navigation Pane](./Includes/images/expand_main_navigation.png) icon at the top left of your workspace) right-click on **Jobs & Pipelines** and select **Open Link in a New Tab**.
-- MAGIC
-- MAGIC    b. This should take you to the pipelines you have created. You should see your **2 - Developing a Simple Pipeline Project** pipeline.
-- MAGIC
-- MAGIC    c. Select your **2 - Developing a Simple Pipeline Project**. Here, you can use the UI to modify the pipeline.
-- MAGIC
-- MAGIC    d. Select the **Settings** button at the top. This will take you to the settings within the UI.
-- MAGIC
-- MAGIC    e. Select **Schedule** to schedule the pipeline. Select **Cancel**, we will learn how to schedule the pipeline later.
-- MAGIC
-- MAGIC    f. Under your pipeline name, select the drop-down with the time date stamp. Here you can view the **Pipeline graph** and other metrics for each run of the pipeline.
-- MAGIC
-- MAGIC    g. Close the pipeline UI tab you opened.
-- MAGIC
-- MAGIC    ![Jobs & Pipelines](./Includes/images/demo_2_view_in_jobs_pipelines.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Additional Resources
-- MAGIC
-- MAGIC - [Lakeflow Spark Declarative Pipelines](https://docs.databricks.com/aws/en/dlt/) documentation.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
