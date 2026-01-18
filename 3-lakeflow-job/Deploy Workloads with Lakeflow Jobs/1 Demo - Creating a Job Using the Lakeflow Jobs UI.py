# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img
# MAGIC     src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png"
# MAGIC     alt="Databricks Learning"
# MAGIC   >
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 1 - Creating a Job Using the Lakeflow Jobs UI
# MAGIC
# MAGIC In this lesson, we will start by creating a job using a single notebook and SQL Query and exploring the Lakeflow Jobs UI.
# MAGIC
# MAGIC In this demonstration, we will walk through the process of creating and running a Lakeflow Job in Databricks. 
# MAGIC
# MAGIC The demo will include:
# MAGIC
# MAGIC - Creating a new job with two tasks: one using a notebook and the other using a SQL query.
# MAGIC - Modifying task configurations.
# MAGIC - Exploring the Lakeflow Jobs UI to understand how to modify, monitor, and manage job runs.
# MAGIC
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC - Schedule a notebook task and Sql task in a Databricks Workflow Job
# MAGIC - Running a Job which have multiple task
# MAGIC
# MAGIC ## Data Overview 
# MAGIC We are going to use a retail dataset for this course across all demos. We have three different dimensions/data available: **customers data, sales data, and orders data** for our retail dataset.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Open marketplace and add delta share with 
# MAGIC 1. **Bank Loan Modelling Dataset**
# MAGIC 1. **Simulated Retail Customer Data**
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Explore Your Environment

# COMMAND ----------

# MAGIC %md
# MAGIC ### B1. Explore your Class Schema
# MAGIC
# MAGIC explore your **workspace.default** schema
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### B2. Explore your Source Catalogs
# MAGIC
# MAGIC #### Bank Loan Modelling Dataset Catalog
# MAGIC
# MAGIC Complete the following to explore your **Bank Loan Modelling Dataset** and **dbacademy_retail** catalogs. We will be ingesting tables and files from these locations during the demos and labs:
# MAGIC
# MAGIC 1. In the left navigation bar, select the catalog icon:  ![Catalog Icon](./Includes/images/catalog_icon.png)
# MAGIC
# MAGIC 2. Locate the catalog called **Bank Loan Modelling Dataset** and expand the catalog.
# MAGIC
# MAGIC 3. Expand your **v01** schema. 
# MAGIC
# MAGIC 4. Notice that within your schema a single volume named **banking** exists with a CSV file.
# MAGIC
# MAGIC #### databricks_simulated_retail_customer_data
# MAGIC
# MAGIC 1. In the left navigation bar, select the catalog icon:  ![Catalog Icon](./Includes/images/catalog_icon.png)
# MAGIC
# MAGIC 2. Locate the catalog called **databricks_simulated_retail_customer_data** and expand the catalog.
# MAGIC
# MAGIC 3. Expand your **v01** schema. 
# MAGIC
# MAGIC 4. Notice that within your schema:
# MAGIC   - Multiple tables exist
# MAGIC   - In **Volumes** two volumes exist: **retail-pipeline** and **source_files**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Viewing Your Files 
# MAGIC Complete the following steps to review the notebook and SQL file you will use in this job. All files are located in the **Task Files** folder within the directory for the corresponding lesson number.
# MAGIC
# MAGIC ### C1. Viewing Notebook File
# MAGIC 1. Navigate to (or click the link for) the notebook: [Task Files/Lesson 1 Files/1.1 - Creating orders table]($./Task Files/Lesson 1 Files/1.1 - Creating orders table).  
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Create the Job
# MAGIC
# MAGIC Complete the steps below to create a Lakeflow Job with two tasks:
# MAGIC
# MAGIC - A notebook task  
# MAGIC - A SQL file task
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### D2. Create and Name the Job
# MAGIC
# MAGIC Complete the following steps to create and name your job.
# MAGIC
# MAGIC 1. Right-click the **Jobs and Pipelines** button in the sidebar and select *Open Link in New Tab*.
# MAGIC
# MAGIC 2. In the new tab, confirm that you are in the **Jobs & Pipelines** tab.
# MAGIC
# MAGIC 3. Click the **Create** button and select **Job** from the dropdown.
# MAGIC
# MAGIC 4. In the top-left corner of the screen, you’ll see a default job name based on the current date and time (for example, *New Job Jul 29, 2025, 11:46 AM*).
# MAGIC
# MAGIC 5. Change the **Job Name** to the one provided in the previous cell (for example: **Demo_01_Retail_Job_labuser123**).
# MAGIC
# MAGIC 6. Leave the job open and proceed to the next steps.
# MAGIC
# MAGIC **NOTE:** If you click on a recommended task (like **Notebook**), you will be redirected to a different page than shown in the screenshot below.
# MAGIC
# MAGIC ![Lesson01_Jobs_UI.png](./Includes/images/Lesson01_Jobs_UI.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### D3. Create the Notebook Task
# MAGIC
# MAGIC Complete the following steps to add a notebook task.
# MAGIC
# MAGIC 1. In the Lakeflow Jobs UI, You may see some task suggestion. For Eg., **Notebook** or **SQL File**
# MAGIC
# MAGIC 2. Select the **Notebook** task type.
# MAGIC
# MAGIC 3. Configure the task using the settings below:
# MAGIC
# MAGIC | Setting         | Instructions |
# MAGIC |-----------------|--------------|
# MAGIC | **Task name**   | Enter **ingesting_orders** |
# MAGIC | **Type**        | Select **Notebook** |
# MAGIC | **Source**      | Choose **Workspace** |
# MAGIC | **Path**        | Use the file navigator to locate and select **Notebook #1**:<br>**./Task Files/Lesson 1 Files/1.1 - Creating orders table** |
# MAGIC | **Compute**     | Select a **Serverless** cluster from the dropdown menu.<br>(We will use Serverless clusters for all jobs in this course. You may specify a different cluster outside of this course, if needed.) <br></br>**NOTE**: If you selected your all-purpose cluster, you may get a warning about how this will be billed as all-purpose compute. Production jobs should always be scheduled against new job clusters appropriately sized for the workload, as this is billed at a much lower rate.
# MAGIC  |
# MAGIC | **Create task** | Click **Create task** |
# MAGIC
# MAGIC 4. Keep the Lakeflow Jobs UI open, you’ll be adding another task in the next step.
# MAGIC ##### For better performance, please enable Performance Optimized Mode in Job Details. Otherwise, it might take 6 to 8 minutes to initiate execution.
# MAGIC
# MAGIC <br></br>
# MAGIC
# MAGIC #### Notebook Task Setup
# MAGIC
# MAGIC ![Lesson01_Notebook_task.png](./Includes/images/Lesson01_Notebook_task.png)
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### D4. Create the SQL Query Task
# MAGIC
# MAGIC Follow these steps to add a SQL file as a task:
# MAGIC
# MAGIC 1. In the Lakeflow Jobs UI, click **Add task**.
# MAGIC
# MAGIC 2. Select the **SQL query** task type.
# MAGIC
# MAGIC 3. Configure the task using the settings below:
# MAGIC
# MAGIC | Setting           | Instructions |
# MAGIC |-------------------|--------------|
# MAGIC | **Task name**     | Enter **ingesting_sales** |
# MAGIC | **Type**          | Select **SQL** |
# MAGIC | **SQL task**      | Select **Query** |
# MAGIC | **SQL query**     | From the dropdown, choose the SQL file:<br>**1.2 - Creating sales table - SQL Query** |
# MAGIC | **SQL warehouse** | From the dropdown, select your SQL warehouse from drop-down menu |
# MAGIC | **Depends on**    | No task should be selected here.<br>(Unselect **ingesting_orders** if it is selected.) |
# MAGIC | **Create task**   | Click **Create task** |
# MAGIC
# MAGIC <br></br>
# MAGIC
# MAGIC #### SQL Task Setup
# MAGIC
# MAGIC ![Lesson01_task1_sql.png](./Includes/images/Lesson01_task1_sql.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### D5. Explore and Modify the Job Details
# MAGIC
# MAGIC 1. Navigate to the Job Details page. In the right pane, you will find the following job-level details:
# MAGIC
# MAGIC - **Job Details:** Information such as Job ID, creator, and more.
# MAGIC - **Schedulers and Triggers:** View and configure various scheduling options and triggers for the job.
# MAGIC - **Job Parameters:** Options to declare parameters that apply to the entire job.
# MAGIC
# MAGIC
# MAGIC #### For better performance, please turn on Performance Optimized Mode in Job Details.
# MAGIC
# MAGIC ##### Performance Optimized Mode
# MAGIC - Enables fast compute startup and improved execution speed.
# MAGIC
# MAGIC ##### Standard Mode
# MAGIC - Disabling performance optimization results in startup times similar to Classic infrastructure and may reduce your costs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Run the Job
# MAGIC
# MAGIC 1. In the upper-right corner, find the kebab menu (three dots) next to the **Run now** button. You will see options such as **Edit as YAML**, **Clone job**, **View as code**, and **Delete job**.
# MAGIC
# MAGIC 2. Click **View as code** to see your job represented in three formats: YAML, Python (SDK and DABS), and JSON.
# MAGIC
# MAGIC 3. Return to the main job page and click the **Run now** button in the top right to start the job.
# MAGIC
# MAGIC     **NOTE:** After starting the job, you can click the link to view the run in progress. In the next section, you will learn another way to view past and current job runs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Review the Job Run
# MAGIC
# MAGIC 1. On the Job Details page, click the **Runs** tab in the top-left corner of the screen (you should currently be on the **Tasks** tab).
# MAGIC
# MAGIC 2. In the Runs tab of your job, you can see detailed information about each run.
# MAGIC    At the top, there is a time-based bar chart where:
# MAGIC
# MAGIC    - The X-axis represents each run.
# MAGIC    - The Y-axis shows the time taken by each task within that run.
# MAGIC 3. Color Coding
# MAGIC    -    key: green = success
# MAGIC    -    red = failed
# MAGIC    -    yellow = waiting/retry, 
# MAGIC    -    pink = skipped,
# MAGIC    -    grey = pending/canceled/timeout.
# MAGIC
# MAGIC
# MAGIC Below the chart, you will find a tabular matrix view that provides the same information in detail. This table starts with the timestamp and includes fields such as run_id, run status, duration, and other relevant details for each run.
# MAGIC
# MAGIC ![Lesson01_view_runs.png](./Includes/images/Lesson01_view_runs.png)
# MAGIC
# MAGIC 4. Open the output details by clicking the timestamp under the **Start time** column:
# MAGIC
# MAGIC    - If **the job is still running**, you will see the active state with a **Status** of **Pending** or **Running** in the right-side panel.
# MAGIC
# MAGIC    - If **the job has completed**, you will see the full execution results with a **Status** of **Succeeded** or **Failed** in the right-side panel.

# COMMAND ----------

# MAGIC %md
# MAGIC ## G. View Your New Tables
# MAGIC 1. From left-hand pane, select **Catalog**. Then drill down from **dbacademy** catalog.
# MAGIC
# MAGIC 2. Expand your unique schema name.
# MAGIC
# MAGIC 3. Notice that within your schema a table named **sales_bronze** and **orders_bronze**

# COMMAND ----------

# MAGIC %md
# MAGIC ##H. Query Your New Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Querying sales_bronze table
# MAGIC SELECT * 
# MAGIC FROM sales_bronze
# MAGIC LIMIT 50;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Querying orders_bronze table
# MAGIC SELECT * 
# MAGIC FROM orders_bronze
# MAGIC LIMIT 50;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC
# MAGIC - [Lakeflow Jobs Documentation](https://docs.databricks.com/aws/en/jobs/)

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
