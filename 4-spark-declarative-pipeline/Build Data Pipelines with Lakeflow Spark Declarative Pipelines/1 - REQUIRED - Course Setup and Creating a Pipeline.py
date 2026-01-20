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
# MAGIC # 1 - REQUIRED - Course Setup and Creating a Pipeline
# MAGIC
# MAGIC In this demo, we'll set up the course environment, explore its components, build a traditional ETL pipeline using JSON files as the data source, and then learn how to create a sample Lakeflow Spark Declarative Pipeline (SDP).
# MAGIC
# MAGIC ### Learning Objectives
# MAGIC
# MAGIC By the end of this lesson, you will be able to:
# MAGIC - Efficiently navigate the Workspace to locate course catalogs, schemas, and source files.
# MAGIC - Create a Lakeflow Spark Declarative Pipeline using the Workspace and the Pipeline UI.
# MAGIC
# MAGIC
# MAGIC ### IMPORTANT - PLEASE READ!
# MAGIC - **REQUIRED** - This notebook is required for all users to run. If you do not run this notebook, you will be missing the necessary files and schemas required for the rest of the course.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Classroom Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Open marketplace and get instant access with 
# MAGIC 1. **Bank Loan Modelling Dataset**
# MAGIC ![](./Includes/Bank Loan Modelling Dataset.png)
# MAGIC 1. **Simulated Retail Customer Data**
# MAGIC ![](./Includes/Simulated Retail Customer Data.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Run these function

# COMMAND ----------

def create_volume(in_catalog: str, in_schema: str, volume_name: str):
    '''
    Create a volume in the specified catalog.schema.
    '''
    print(f'Creating volume: {in_catalog}.{in_schema}.{volume_name} if not exists.\n')
    r = spark.sql(f'CREATE VOLUME IF NOT EXISTS {in_catalog}.{in_schema}.{volume_name}')

def create_schemas(in_catalog: str, schema_names: list):
    '''
    Create schemas for the course in the specified catalog. Use DA.catalog_name in vocareum.

    If the schemas do not exist in the environment it will creates the schemas based the user's schema_name list.

    Parameters:
    - schema_names (list): A list of strings representing schema names to creates.

    Returns:
        Log information:
            - If schemas(s) do not exist, prints information on the schemas it created.
            - If schemas(s) exist, prints information that schemas exist.

    Example:
    -------
    - create_schemas(in_catalog = DA.catalog_name, schema_names = ['1_bronze', '2_silver', '3_gold'])
    '''

    ## Current schemas in catalog
    list_of_curr_schemas = spark.sql(f'SHOW SCHEMAS IN {in_catalog}').toPandas().databaseName.to_list()

    # Create schema in catalog if not exists
    for schema in schema_names:
        if schema not in list_of_curr_schemas:
            print(f'Creating schema: {in_catalog}.{schema}.')
            spark.sql(f'CREATE SCHEMA IF NOT EXISTS {in_catalog}.{schema}')
        else:
            print(f'Schema {in_catalog}.{schema} already exists. No action taken.')
def delete_source_files(source_files: str):
    """
    Deletes all files in the specified source volume.

    This function iterates through all the files in the given volume,
    deletes them, and prints the name of each file being deleted.

    Parameters:
    - source_files : str
        The path to the volume containing the files to delete. 
        Use the {DA.paths.working_dir} to dynamically navigate to the user's volume location in dbacademy/ops/vocareumlab@name:
            Example: DA.paths.working_dir = /Volumes/dbacademy/ops/vocareumlab@name

    Returns:
    - None. This function does not return any value. It performs file deletion and prints all files that it deletes. If no files are found it prints in the output.

    Example:
    - delete_source_files(f'{DA.paths.working_dir}/pii/stream_source/user_reg')
    """

    import os

    print(f'\nSearching for files in {source_files} volume to delete prior to creating files...')
    if os.path.exists(source_files):
        list_of_files = sorted(os.listdir(source_files))
    else:
        list_of_files = None

    if not list_of_files:  # Checks if the list is empty.
        print(f"No files found in {source_files}.\n")
    else:
        for file in list_of_files:
            file_to_delete = source_files + file
            print(f'Deleting file: {file_to_delete}')
            dbutils.fs.rm(file_to_delete)
def copy_files(copy_from: str, copy_to: str, n: int, sleep=2):
    '''
    Copy files from one location to another destination's volume.

    This method performs the following tasks:
      1. Lists files in the source directory and sorts them. Sorted to keep them in the same order when copying for consistency.
      2. Verifies that the source directory has at least `n` files.
      3. Copies files from the source to the destination, skipping files already present at the destination.
      4. Pauses for `sleep` seconds after copying each file.
      5. Stops after copying `n` files or if all files are processed.
      6. Will print information on the files copied.
    
    Parameters
    - copy_from (str): The source directory where files are to be copied from.
    - copy_to (str): The destination directory where files will be copied to.
    - n (int): The number of files to copy from the source. If n is larger than total files, an error is returned.
    - sleep (int, optional): The number of seconds to pause after copying each file. Default is 2 seconds.

    Returns:
    - None: Prints information to the log on what files it's loading. If the file exists, it skips that file.

    Example:
    - copy_files(copy_from='/Volumes/gym_data/v01/user-reg', 
           copy_to=f'{DA.paths.working_dir}/pii/stream_source/user_reg',
           n=1)
    '''
    import os
    import time

    print(f"\n----------------Loading files to user's volume: '{copy_to}'----------------")

    ## List all files in the copy_from volume and sort the list
    list_of_files_to_copy = sorted(os.listdir(copy_from))
    total_files_in_copy_location = len(list_of_files_to_copy)

    ## Get a list of files in the source
    list_of_files_in_source = os.listdir(copy_to)

    assert total_files_in_copy_location >= n, f"The source location contains only {total_files_in_copy_location} files, but you specified {n}  files to copy. Please specify a number less than or equal to the total number of files available."

    ## Looping counter
    counter = 1

    ## Load files if not found in the co
    for file in list_of_files_to_copy:

      ## If the file is found in the source, skip it with a note. Otherwise, copy file.
      if file in list_of_files_in_source:
        print(f'File number {counter} - {file} is already in the source volume "{copy_to}". Skipping file.')
      else:
        file_to_copy = f'{copy_from}/{file}'
        copy_file_to = f'{copy_to}/{file}'
        print(f'File number {counter} - Copying file {file_to_copy} --> {copy_file_to}.')
        dbutils.fs.cp(file_to_copy, copy_file_to , recurse = True)
        
        ## Sleep after load
        time.sleep(sleep) 

      ## Stop after n number of loops based on argument.
      if counter == n:
        break
      else:
        counter = counter + 1
def copy_file_for_multiple_sources(copy_n_files = 2, 
                                   sleep_set = 3,
                                   copy_from_source=str,
                                   copy_to_target=str
                                   ):

    for n in range(copy_n_files):
        n = n + 1
        copy_files(copy_from = f'{copy_from_source}/orders/stream_json', copy_to = f'{copy_to_target}/orders', n = n, sleep=sleep_set)
        copy_files(copy_from = f'{copy_from_source}/customers/stream_json', copy_to = f'{copy_to_target}/customers', n = n, sleep=sleep_set)
        copy_files(copy_from = f'{copy_from_source}/status/stream_json', copy_to = f'{copy_to_target}/status', n = n, sleep=sleep_set)
import os
def create_directory_in_user_volume(user_default_volume_path: str, create_folders: list):
    '''
    Creates multiple (or single) directories in the specified volume path.

    Parameters:
    - user_default_volume_path (str): The base directory path where the folders will be created. 
                                      You can use the default DA.paths.working_dir as the user's volume path.
    - create_folders (list): A list of strings representing folder names to be created within the base directory.

    Returns:
    - None: This function does not return any values but prints log information about the created directories.

    Example: 
    - create_directory_in_user_volume(user_default_volume_path=DA.paths.working_dir, create_folders=['customers', 'orders', 'status'])
    '''
    
    print('----------------------------------------------------------------------------------------')
    for folder in create_folders:

        create_folder = f'{user_default_volume_path}/{folder}'

        if not os.path.exists(create_folder):
        # If it doesn't exist, create the directory
            dbutils.fs.mkdirs(create_folder)
            print(f'Creating folder: {create_folder}')

        else:
            print(f"Directory {create_folder} already exists. No action taken.")
        
    print('----------------------------------------------------------------------------------------\n')
def setup_complete():
  '''
  Prints a note in the output that the setup was complete.
  '''
  print('\n\n\n------------------------------------------------------------------------------')
  print('SETUP COMPLETE!')
  print('------------------------------------------------------------------------------')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Run the functions
# MAGIC

# COMMAND ----------

catalog_name = "pipeline"
schema_name = ['pipeline_data', '1_bronze_db', '2_silver_db', '3_gold_db']
volume_name = "data"
data_volume_path = f"/Volumes/{catalog_name}/{schema_name[0]}/{volume_name}"
## Create catalogs for the course
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
create_schemas(in_catalog = catalog_name, schema_names = schema_name)

## Create volume for the course
create_volume(in_catalog=catalog_name, in_schema=schema_name[0], volume_name=volume_name)

## Create directories in the specified volume
create_directory_in_user_volume(user_default_volume_path = data_volume_path, create_folders = ['customers', 'orders', 'status'])


## Delete all files in the labuser's volume to reset the class if necessary. Otherwise does nothing.
delete_source_files(f'{data_volume_path}/customers/')
delete_source_files(f'{data_volume_path}/orders/')
delete_source_files(f'{data_volume_path}/status/')


##
## Start each directory in the labuser's volume with one file.
##

## Customers
copy_files(copy_from='/Volumes/databricks_simulated_retail_customer_data/v01/retail-pipeline/customers/stream_json/', 
           copy_to=f'{data_volume_path}/customers', 
           n=1)

## Orders
copy_files(copy_from='/Volumes/databricks_simulated_retail_customer_data/v01/retail-pipeline/orders/stream_json/', 
           copy_to=f'{data_volume_path}/orders', 
           n=1)

## Customers
copy_files(copy_from='/Volumes/databricks_simulated_retail_customer_data/v01/retail-pipeline/status/stream_json/', 
           copy_to=f'{data_volume_path}/status', 
           n=1)



setup_complete()

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Explore the Lab Environment
# MAGIC
# MAGIC Explore the raw data source files, catalogs, and schema in the course lab environment.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Complete these steps to explore your user catalog and schemas you will be using in this course:
# MAGIC
# MAGIC    - a. Select the **Catalog** icon ![Catalog Icon](./Includes/images/catalog_icon.png) in the left navigation bar.
# MAGIC
# MAGIC    - b. You should see your unique catalog, named something like **pipeline**. You will use this catalog throughout the course.
# MAGIC
# MAGIC    - c. Expand your **pipeline** catalog. It should contain the following schemas:
# MAGIC      - **1_bronze_db**
# MAGIC      - **2_silver_db**
# MAGIC      - **3_gold_db**
# MAGIC      - **pipeline_data**

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Complete the following steps to view where our streaming raw source files are coming from:
# MAGIC
# MAGIC    a. Select the **Catalog** icon ![Catalog Icon](./Includes/images/catalog_icon.png) in the left navigation bar.
# MAGIC
# MAGIC    b. Expand the **pipeline** catalog.
# MAGIC
# MAGIC    c. Expand the **pipeline_data** schema and then **Volumes**.
# MAGIC
# MAGIC    d. Expand your **data** volume. You should notice that your volume contains three folders:
# MAGIC    - **customers**
# MAGIC    - **orders**
# MAGIC    - **status**
# MAGIC
# MAGIC    e. Expand each folder and notice that each cloud storage location contains a single JSON file to start with.

# COMMAND ----------

## Set working dir
working_dir = data_volume_path


# COMMAND ----------

## With Python
print(working_dir)

# COMMAND ----------

# MAGIC %sql
# MAGIC DECLARE VARIABLE path_working_dir STRING DEFAULT '/Volumes/pipeline/pipeline_data/data';

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Build a Traditional ETL Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Query the raw JSON file(s) in your `/Volumes/pipeline/pipeline_data/data` volume to preview the data. 
# MAGIC
# MAGIC       Notice that the JSON file is displayed ingested into tabular form using the `read_files` function. Take note of the following:
# MAGIC
# MAGIC     a. The **orders** JSON file contains order data for a company.
# MAGIC
# MAGIC     b. The one JSON file in your **/orders** volume (**00.json**) contains 174 rows. Remember that number for later.

# COMMAND ----------

spark.sql(f'''
          SELECT * 
          FROM json.`{working_dir}/orders`
          ''').display()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Traditionally, you would build an ETL pipeline by reading all of the files within the cloud storage location each time the pipeline runs. As data scales, this method becomes inefficient, more expensive, and time-consuming.
# MAGIC
# MAGIC    For example, you would write code like below.
# MAGIC
# MAGIC    **NOTES:** 
# MAGIC    - The tables and views will be written to your **labuser.default** schema (database).
# MAGIC    - Knowledge of the Databricks `read_files` function is prerequisite for this course.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- JSON -> Bronze
# MAGIC -- Read ALL files from your working directory each time the query is executed
# MAGIC CREATE OR REPLACE TABLE default.orders_bronze
# MAGIC AS 
# MAGIC SELECT 
# MAGIC   *,
# MAGIC   current_timestamp() AS processing_time,
# MAGIC   _metadata.file_name AS source_file
# MAGIC FROM read_files(
# MAGIC     path_working_dir || "/orders", 
# MAGIC     format =>"json");
# MAGIC
# MAGIC
# MAGIC -- Bronze -> Silver
# MAGIC -- Read the entire bronze table each time the query is executed
# MAGIC CREATE OR REPLACE TABLE default.orders_silver
# MAGIC AS 
# MAGIC SELECT 
# MAGIC   order_id,
# MAGIC   timestamp(order_timestamp) AS order_timestamp, 
# MAGIC   customer_id,
# MAGIC   notifications
# MAGIC FROM default.orders_bronze;   
# MAGIC
# MAGIC
# MAGIC -- Silver -> Gold
# MAGIC -- Aggregate the silver each time the query is executed.
# MAGIC CREATE OR REPLACE VIEW default.orders_by_date_vw     
# MAGIC AS 
# MAGIC SELECT 
# MAGIC   date(order_timestamp) AS order_date, 
# MAGIC   count(*) AS total_daily_orders
# MAGIC FROM default.orders_silver                               
# MAGIC GROUP BY date(order_timestamp);

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Run the code in the cells to view the **orders_bronze** and **orders_silver** tables, and the **orders_by_date_vw** view. Explore the results.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM default.orders_bronze
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM default.orders_silver
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM default.orders_by_date_vw
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Considerations
# MAGIC
# MAGIC - As JSON files are added to the volume in cloud storage, your **bronze table** code will read **all** of the files each time it executes, rather than reading only new rows of raw data. As the data grows, this can become inefficient and costly.
# MAGIC
# MAGIC - The **silver table** code will always read all the rows from the bronze table to prepare the silver table. As the data grows, this can also become inefficient and costly.
# MAGIC
# MAGIC - The traditional view, **orders_by_date_vw**, executes each time it is called. As the data grows, this can become inefficient.
# MAGIC
# MAGIC - To check data quality as new rows are added, additional code is needed to identify any values that do not meet the required conditions.
# MAGIC
# MAGIC - Monitoring the pipeline for each run is a challenge.
# MAGIC
# MAGIC - There is no simple user interface to explore, monitor, or fix issues everytime the code runs.
# MAGIC
# MAGIC ### We can automatically process data incrementally, manage infrastructure, monitor, observe, optimize, and view this ETL pipeline by converting this to use **Spark Declarative Pipelines**!

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Get Started Creating a Lakeflow Spark Declarative Pipeline Using the New Lakeflow Pipelines Editor
# MAGIC
# MAGIC In this section, we'll show you how to start creating a Spark Declarative Pipeline using the new Lakeflow Pipelines Editor. We won't run or modify the pipeline just yet!
# MAGIC
# MAGIC There are a few different ways to create your pipeline. Let's explore these methods.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. First, complete the following steps to enable the new **Lakeflow Pipelines Editor**:
# MAGIC
# MAGIC    **NOTE:** This is being updated and how to enable it might change slightly moving forward.
# MAGIC
# MAGIC    a. In the top-right corner, select your user icon .
# MAGIC
# MAGIC    b. Right-click on **Settings** and select **Open in New Tab**.
# MAGIC
# MAGIC    c. Select **Developer**.
# MAGIC
# MAGIC    d. Scroll to the bottom and enable **Lakeflow Pipelines Editor** if it's not enabled and Click **Enable tabs for notebooks and files**.
# MAGIC
# MAGIC    e. Refresh your browser page to enable the option you turned on.

# COMMAND ----------

# MAGIC %md
# MAGIC ### D1. Create a Spark Declarative Pipeline Using the File Explorer
# MAGIC 1. Complete the following steps to create a Spark Declarative Pipeline using the left navigation pane:
# MAGIC
# MAGIC    a. In the left navigation bar, select the **Folder** ![Folder Icon](./Includes/images/folder_icon.png) icon to open the Workspace navigation.
# MAGIC
# MAGIC    b. Navigate to the **Build Data Pipelines with Lakeflow Spark Declarative Pipelines** folder (you are most likely already there).
# MAGIC
# MAGIC    c. (**PLEASE READ**) To complete this demonstration, it'll be easier to open this same notebook in another tab to follow along with these instructions. Right click on the notebook **1 - REQUIRED - Course Setup and Creating a Pipeline** and select **Open in a New Tab**.
# MAGIC
# MAGIC    d. In the other tab select the three ellipsis icon ![Ellipsis Icon](./Includes/images/ellipsis_icon.png) in the folder navigation bar.
# MAGIC
# MAGIC    e. Select **Create** -> **ETL Pipeline**:
# MAGIC       - If you have not enabled the new **Lakeflow Pipelines Editor** a pop-up might appear asking you to enable the new editor. Select **Enable** here or complete the previous step.
# MAGIC
# MAGIC       </br>
# MAGIC
# MAGIC       - Then use the following information:
# MAGIC
# MAGIC          - **Name**: `my-pipeline-project`
# MAGIC
# MAGIC          - **Default catalog**: Select your **pipeline** catalog
# MAGIC
# MAGIC          - **Default schema**: Select your **default** schema (database)
# MAGIC
# MAGIC          - Select **Start with sample code in SQL**
# MAGIC
# MAGIC          The project will open up in the pipeline editor and look like the following:
# MAGIC
# MAGIC
# MAGIC    f. This will open your Spark Declarative Pipeline within the **Lakeflow Pipelines Editor**. By default, the project creates multiple folders and sample files for you as a starter. You can use this sample folder structure or create your own. Notice the following in the pipeline editor:
# MAGIC
# MAGIC       - The Spark Declarative Pipeline is located within the **Pipeline** tab.
# MAGIC
# MAGIC       - Here, you start with a sample project and folder structure.
# MAGIC
# MAGIC       - To navigate back to all your files and folders, select **All Files**.
# MAGIC
# MAGIC       - We will explore the pipeline editor and running a pipeline in the next demonstration.
# MAGIC
# MAGIC    g. Close the link with the sample pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ### D2. Create a Spark Declarative Pipeline Using the Pipeline UI
# MAGIC 1. You can also create a Spark Declarative Pipeline using the far-left main navigation bar by completing the following steps:
# MAGIC
# MAGIC    a. On the far-left navigation bar, right-click **Jobs and Pipelines** and select **Open Link in New Tab**.
# MAGIC
# MAGIC    b. Find the blue **Create** button and select it.
# MAGIC
# MAGIC    c. Select **ETL pipeline**.
# MAGIC
# MAGIC    d. The same **Create pipeline** pop-up appears as before. 
# MAGIC
# MAGIC    e. Here select **Add existing assets**. 
# MAGIC
# MAGIC    f. The **Add existing assets** button enables you to select a folder with pipeline assets. This option will enable you to associate this new pipeline with code files already available in your Workspace, including Git folders.
# MAGIC
# MAGIC    <img src="./Includes/images/existing_assets.png" alt="Existing Assets" width="400">
# MAGIC
# MAGIC
# MAGIC    g. You can close out of the pop up window and close the pipeline tab. You do not need to select a folder yet.

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
