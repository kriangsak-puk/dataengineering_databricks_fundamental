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
-- MAGIC # 6 - Change Data Capture with AUTO CDC with Slowing Changing Dimensions (SCD) TYPE 1
-- MAGIC
-- MAGIC ##### NOTE: The AUTO CDC APIs replace the APPLY CHANGES APIs, and have the same syntax. The APPLY CHANGES APIs are still available, but Databricks recommends using the AUTO CDC APIs in their place.
-- MAGIC
-- MAGIC In this demonstration, we will continue to build our pipeline by ingesting **customer** data into our pipeline. The customer data includes new customers, customers who have deleted their accounts, and customers who have updated their information (such as address, email, etc.). We will need to build our customer pipeline by implementing change data capture (CDC) for customer data using SCD Type 1 (Type 2 is outside the scope of this course).
-- MAGIC
-- MAGIC The customer pipeline flow will:
-- MAGIC
-- MAGIC - The bronze table uses **Auto Loader** to ingest JSON data from cloud object storage with SQL (`FROM STREAM`).
-- MAGIC - A table is defined to enforce constraints before passing records to the silver layer.
-- MAGIC - `AUTO CDC` is used to automatically process CDC data into the silver layer as a Type 1.
-- MAGIC - A gold table is defined to create a materialized view of the current customers with updated information (dropped customers, new customers and updated customer information).
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ### Learning Objectives
-- MAGIC
-- MAGIC By the end of this lesson, students should feel comfortable:
-- MAGIC - Apply the `AUTO CDC` operation in Lakeflow Spark Declarative Pipelines to process change data capture (CDC) by integrating and updating incoming data from a source stream into an existing Delta table, ensuring data accuracy and consistency.
-- MAGIC - Analyze Slowly Changing Dimensions (SCD Type 1) tables within Lakeflow Spark Declarative Pipelines to effectively update, insert and drop customers in dimensional data, managing the state of records over time using appropriate keys, versioning, and timestamps.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Setup
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import json
-- MAGIC import os
-- MAGIC import json
-- MAGIC from databricks.sdk import WorkspaceClient
-- MAGIC
-- MAGIC
-- MAGIC def create_declarative_pipeline(pipeline_name: str, 
-- MAGIC                         root_path_folder_name: str,
-- MAGIC                         source_folder_names: list = [],
-- MAGIC                         catalog_name: str = 'dbacademy',
-- MAGIC                         schema_name: str = 'default',
-- MAGIC                         serverless: bool = True,
-- MAGIC                         configuration: dict = {},
-- MAGIC                         continuous: bool = False,
-- MAGIC                         photon: bool = True,
-- MAGIC                         channel: str = 'PREVIEW',
-- MAGIC                         development: bool = True,
-- MAGIC                         pipeline_type = 'WORKSPACE'
-- MAGIC                         ):
-- MAGIC   
-- MAGIC     '''
-- MAGIC   Creates the specified DLT pipeline.
-- MAGIC
-- MAGIC   Parameters:
-- MAGIC   ----------
-- MAGIC   pipeline_name : str
-- MAGIC       The name of the DLT pipeline to be created.
-- MAGIC   root_path_folder_name : str
-- MAGIC       The root folder name where the pipeline will be located. This folder must be in the location where this function is called.
-- MAGIC   source_folder_names : list, optional
-- MAGIC       A list of source folder names. Must defined at least one folder within the root folder location above.
-- MAGIC   catalog_name : str, optional
-- MAGIC       The catalog name for the DLT pipeline. Default is 'dbacademy'.
-- MAGIC   schema_name : str, optional
-- MAGIC       The schema name for the DLT pipeline. Default is 'default'.
-- MAGIC   serverless : bool, optional
-- MAGIC       If True, the pipeline will be serverless. Default is True.
-- MAGIC   configuration : dict, optional
-- MAGIC       A dictionary of configuration settings for the pipeline. Default is an empty dictionary.
-- MAGIC   continuous : bool, optional
-- MAGIC       If True, the pipeline will be run in continuous mode. Default is False.
-- MAGIC   photon : bool, optional
-- MAGIC       If True, the pipeline will use Photon for processing. Default is True.
-- MAGIC   channel : str, optional
-- MAGIC       The channel for the pipeline, such as 'PREVIEW'. Default is 'PREVIEW'.
-- MAGIC   development : bool, optional
-- MAGIC       If True, the pipeline will be set up for development. Default is True.
-- MAGIC   pipeline_type : str, optional
-- MAGIC       The type of the pipeline (e.g., 'WORKSPACE'). Default is 'WORKSPACE'.
-- MAGIC
-- MAGIC   Returns:
-- MAGIC   -------
-- MAGIC   None
-- MAGIC       This function does not return anything. It creates the DLT pipeline based on the provided parameters.
-- MAGIC
-- MAGIC   Example:
-- MAGIC   --------
-- MAGIC   create_dlt_pipeline(pipeline_name='my_pipeline_name', 
-- MAGIC                       root_path_folder_name='6 - Putting a DLT Pipeline in Production Project',
-- MAGIC                       source_folder_names=['orders', 'status'])
-- MAGIC   '''
-- MAGIC   
-- MAGIC     w = WorkspaceClient()
-- MAGIC     for pipeline in w.pipelines.list_pipelines():
-- MAGIC         if pipeline.name == pipeline_name:
-- MAGIC             raise ValueError(f"Lakeflow Declarative Pipeline name '{pipeline_name}' already exists. Please delete the pipeline using the UI and rerun the cell to recreate the pipeline.")
-- MAGIC
-- MAGIC     ## Create empty dictionary
-- MAGIC     create_dlt_pipeline_call = {}
-- MAGIC
-- MAGIC     ## Pipeline type
-- MAGIC     create_dlt_pipeline_call['pipeline_type'] = pipeline_type
-- MAGIC
-- MAGIC     ## Modify dictionary for specific DLT configurations
-- MAGIC     create_dlt_pipeline_call['name'] = pipeline_name
-- MAGIC
-- MAGIC     ## Set paths to root and source folders
-- MAGIC     main_course_folder_path = os.getcwd()
-- MAGIC
-- MAGIC     main_path_to_dlt_project_folder = os.path.join('/', main_course_folder_path, root_path_folder_name)
-- MAGIC     create_dlt_pipeline_call['root_path'] = main_path_to_dlt_project_folder
-- MAGIC
-- MAGIC     ## Add path of root folder to source folder names
-- MAGIC     add_path_to_folder_names = [os.path.join(main_path_to_dlt_project_folder, folder_name, '**') for folder_name in source_folder_names]
-- MAGIC     source_folders_path = [{'glob':{'include':folder_name}} for folder_name in add_path_to_folder_names]
-- MAGIC     create_dlt_pipeline_call['libraries'] = source_folders_path
-- MAGIC
-- MAGIC     ## Set default catalog and schema
-- MAGIC     create_dlt_pipeline_call['catalog'] = catalog_name
-- MAGIC     create_dlt_pipeline_call['schema'] = schema_name
-- MAGIC
-- MAGIC     ## Set serverless compute
-- MAGIC     create_dlt_pipeline_call['serverless'] = serverless
-- MAGIC
-- MAGIC     ## Set configuration parameters
-- MAGIC     create_dlt_pipeline_call['configuration'] = configuration
-- MAGIC
-- MAGIC     ## Set if continouous or not
-- MAGIC     create_dlt_pipeline_call['continuous'] = continuous 
-- MAGIC
-- MAGIC     ## Set to use Photon
-- MAGIC     create_dlt_pipeline_call['photon'] = photon
-- MAGIC
-- MAGIC     ## Set DLT channel
-- MAGIC     create_dlt_pipeline_call['channel'] = channel
-- MAGIC
-- MAGIC     ## Set if development mode
-- MAGIC     create_dlt_pipeline_call['development'] = development
-- MAGIC
-- MAGIC     ## Creat DLT pipeline
-- MAGIC
-- MAGIC     print(f"Creating the Lakeflow Declarative Pipeline '{pipeline_name}'...")
-- MAGIC     print(f"Root folder path: {main_path_to_dlt_project_folder}")
-- MAGIC     print(f"Source folder path(s): {source_folders_path}")
-- MAGIC
-- MAGIC     w.api_client.do('POST', '/api/2.0/pipelines', body=create_dlt_pipeline_call)
-- MAGIC     print(f"\nLakeflow Declarative Pipeline Creation '{pipeline_name}' Complete!")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def create_volume(in_catalog: str, in_schema: str, volume_name: str):
-- MAGIC     '''
-- MAGIC     Create a volume in the specified catalog.schema.
-- MAGIC     '''
-- MAGIC     print(f'Creating volume: {in_catalog}.{in_schema}.{volume_name} if not exists.\n')
-- MAGIC     r = spark.sql(f'CREATE VOLUME IF NOT EXISTS {in_catalog}.{in_schema}.{volume_name}')
-- MAGIC
-- MAGIC def create_schemas(in_catalog: str, schema_names: list):
-- MAGIC     '''
-- MAGIC     Create schemas for the course in the specified catalog. Use DA.catalog_name in vocareum.
-- MAGIC
-- MAGIC     If the schemas do not exist in the environment it will creates the schemas based the user's schema_name list.
-- MAGIC
-- MAGIC     Parameters:
-- MAGIC     - schema_names (list): A list of strings representing schema names to creates.
-- MAGIC
-- MAGIC     Returns:
-- MAGIC         Log information:
-- MAGIC             - If schemas(s) do not exist, prints information on the schemas it created.
-- MAGIC             - If schemas(s) exist, prints information that schemas exist.
-- MAGIC
-- MAGIC     Example:
-- MAGIC     -------
-- MAGIC     - create_schemas(in_catalog = DA.catalog_name, schema_names = ['1_bronze', '2_silver', '3_gold'])
-- MAGIC     '''
-- MAGIC
-- MAGIC     ## Current schemas in catalog
-- MAGIC     list_of_curr_schemas = spark.sql(f'SHOW SCHEMAS IN {in_catalog}').toPandas().databaseName.to_list()
-- MAGIC
-- MAGIC     # Create schema in catalog if not exists
-- MAGIC     for schema in schema_names:
-- MAGIC         if schema not in list_of_curr_schemas:
-- MAGIC             print(f'Creating schema: {in_catalog}.{schema}.')
-- MAGIC             spark.sql(f'CREATE SCHEMA IF NOT EXISTS {in_catalog}.{schema}')
-- MAGIC         else:
-- MAGIC             print(f'Schema {in_catalog}.{schema} already exists. No action taken.')
-- MAGIC def delete_source_files(source_files: str):
-- MAGIC     """
-- MAGIC     Deletes all files in the specified source volume.
-- MAGIC
-- MAGIC     This function iterates through all the files in the given volume,
-- MAGIC     deletes them, and prints the name of each file being deleted.
-- MAGIC
-- MAGIC     Parameters:
-- MAGIC     - source_files : str
-- MAGIC         The path to the volume containing the files to delete. 
-- MAGIC         Use the {DA.paths.working_dir} to dynamically navigate to the user's volume location in dbacademy/ops/vocareumlab@name:
-- MAGIC             Example: DA.paths.working_dir = /Volumes/dbacademy/ops/vocareumlab@name
-- MAGIC
-- MAGIC     Returns:
-- MAGIC     - None. This function does not return any value. It performs file deletion and prints all files that it deletes. If no files are found it prints in the output.
-- MAGIC
-- MAGIC     Example:
-- MAGIC     - delete_source_files(f'{DA.paths.working_dir}/pii/stream_source/user_reg')
-- MAGIC     """
-- MAGIC
-- MAGIC     import os
-- MAGIC
-- MAGIC     print(f'\nSearching for files in {source_files} volume to delete prior to creating files...')
-- MAGIC     if os.path.exists(source_files):
-- MAGIC         list_of_files = sorted(os.listdir(source_files))
-- MAGIC     else:
-- MAGIC         list_of_files = None
-- MAGIC
-- MAGIC     if not list_of_files:  # Checks if the list is empty.
-- MAGIC         print(f"No files found in {source_files}.\n")
-- MAGIC     else:
-- MAGIC         for file in list_of_files:
-- MAGIC             file_to_delete = source_files + file
-- MAGIC             print(f'Deleting file: {file_to_delete}')
-- MAGIC             dbutils.fs.rm(file_to_delete)
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
-- MAGIC def copy_file_for_multiple_sources(copy_n_files = 2, 
-- MAGIC                                    sleep_set = 3,
-- MAGIC                                    copy_from_source=str,
-- MAGIC                                    copy_to_target=str
-- MAGIC                                    ):
-- MAGIC
-- MAGIC     for n in range(copy_n_files):
-- MAGIC         n = n + 1
-- MAGIC         copy_files(copy_from = f'{copy_from_source}/orders/stream_json', copy_to = f'{copy_to_target}/orders', n = n, sleep=sleep_set)
-- MAGIC         copy_files(copy_from = f'{copy_from_source}/customers/stream_json', copy_to = f'{copy_to_target}/customers', n = n, sleep=sleep_set)
-- MAGIC         copy_files(copy_from = f'{copy_from_source}/status/stream_json', copy_to = f'{copy_to_target}/status', n = n, sleep=sleep_set)
-- MAGIC import os
-- MAGIC def create_directory_in_user_volume(user_default_volume_path: str, create_folders: list):
-- MAGIC     '''
-- MAGIC     Creates multiple (or single) directories in the specified volume path.
-- MAGIC
-- MAGIC     Parameters:
-- MAGIC     - user_default_volume_path (str): The base directory path where the folders will be created. 
-- MAGIC                                       You can use the default DA.paths.working_dir as the user's volume path.
-- MAGIC     - create_folders (list): A list of strings representing folder names to be created within the base directory.
-- MAGIC
-- MAGIC     Returns:
-- MAGIC     - None: This function does not return any values but prints log information about the created directories.
-- MAGIC
-- MAGIC     Example: 
-- MAGIC     - create_directory_in_user_volume(user_default_volume_path=DA.paths.working_dir, create_folders=['customers', 'orders', 'status'])
-- MAGIC     '''
-- MAGIC     
-- MAGIC     print('----------------------------------------------------------------------------------------')
-- MAGIC     for folder in create_folders:
-- MAGIC
-- MAGIC         create_folder = f'{user_default_volume_path}/{folder}'
-- MAGIC
-- MAGIC         if not os.path.exists(create_folder):
-- MAGIC         # If it doesn't exist, create the directory
-- MAGIC             dbutils.fs.mkdirs(create_folder)
-- MAGIC             print(f'Creating folder: {create_folder}')
-- MAGIC
-- MAGIC         else:
-- MAGIC             print(f"Directory {create_folder} already exists. No action taken.")
-- MAGIC         
-- MAGIC     print('----------------------------------------------------------------------------------------\n')
-- MAGIC def setup_complete():
-- MAGIC   '''
-- MAGIC   Prints a note in the output that the setup was complete.
-- MAGIC   '''
-- MAGIC   print('\n\n\n------------------------------------------------------------------------------')
-- MAGIC   print('SETUP COMPLETE!')
-- MAGIC   print('------------------------------------------------------------------------------')

-- COMMAND ----------

-- MAGIC
-- MAGIC %python
-- MAGIC catalog_name = "pipeline"
-- MAGIC schema_name = ['pipeline_data', '1_bronze_db', '2_silver_db', '3_gold_db']
-- MAGIC volume_name = "data"
-- MAGIC data_volume_path = f"/Volumes/{catalog_name}/{schema_name[0]}/{volume_name}"
-- MAGIC working_dir = data_volume_path
-- MAGIC print(working_dir)
-- MAGIC
-- MAGIC # ## Create volume for the lab
-- MAGIC # create_volume(in_catalog=catalog_name, in_schema = 'default', volume_name = 'lab_staging_files')
-- MAGIC # create_volume(in_catalog=catalog_name, in_schema = 'default', volume_name = 'lab_files')
-- MAGIC
-- MAGIC # ## Create schemas for lab data
-- MAGIC # create_schemas(in_catalog = catalog_name, schema_names = ['lab_1_bronze_db', 'lab_2_silver_db', 'lab_3_gold_db'])
-- MAGIC
-- MAGIC
-- MAGIC # ## Create the country_lookup table if it doesn't exist
-- MAGIC # if spark.catalog.tableExists(f"{catalog_name}.default.country_lookup") == False:
-- MAGIC #     create_country_lookup_table(in_catalog = catalog_name, in_schema = 'default')
-- MAGIC # else:
-- MAGIC #     print(f'Table {catalog_name}.default.country_lookup already exists. No action taken')
-- MAGIC
-- MAGIC # delete_source_files(f'/Volumes/{catalog_name}/default/lab_files/')
-- MAGIC # delete_source_files(f'/Volumes/{catalog_name}/default/lab_files_staging/')
-- MAGIC
-- MAGIC # # Example usage
-- MAGIC # LabSetup = LabDataSetup(f'{catalog_name}','default','lab_staging_files')
-- MAGIC # LabSetup.copy_file(copy_file = 'employees_1.csv', 
-- MAGIC #                    to_target_volume = f'/Volumes/{catalog_name}/default/lab_files')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## B. Explore the Customer Data Source Files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Run the cell below to programmatically view the files in your `/Volumes/dbacademy/ops/lab-user-name/customers` volume. Confirm you only see one **00.json** file for customers.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f'LIST "{working_dir}/customers"').display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Run the query below to explore the customers **00.json** file located at `/Volumes/dbacademy/ops/lab-user-name/customers`. Note the following:
-- MAGIC
-- MAGIC    a. The file contains **939 customers** (remember this number).
-- MAGIC
-- MAGIC    b. It includes general customer information such as **email**, **name**, and **address**.
-- MAGIC
-- MAGIC    c. The **timestamp** column specifies the logical order of customer events in the source data.
-- MAGIC
-- MAGIC    d. The **operation** column indicates whether the entry is for a new customer, a deletion, or an update.
-- MAGIC       - **NOTE:** Since this is the first JSON file, all rows will be considered new customers.
-- MAGIC

-- COMMAND ----------

SELECT *
FROM read_files(
  '/Volumes/pipeline/pipeline_data/data' || '/customers/00.json',
  format => "JSON"
)
ORDER BY operation;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Question: 
-- MAGIC How can we ingest new raw data source files (JSON) with customer updates into our pipeline to update the **customers_silver** table when inserts, updates, or deletes occur, without maintaining historical records (SCD Type 1)?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C. Change Data Capture with AUTO CDC APIs in Lakeflow Spark Declarative Pipelines

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Run the cell below to create your starter Spark Declarative Pipeline for this demonstration. The pipeline will set the following for you:
-- MAGIC     - Your default catalog: `labuser`
-- MAGIC     - Your configuration parameter: `source` = `/Volumes/dbacademy/ops/your-labuser-name`
-- MAGIC
-- MAGIC     **NOTE:** If the pipeline already exists, an error will be returned. In that case, you'll need to delete the existing pipeline and rerun this cell.
-- MAGIC
-- MAGIC **NOTE:**  The `create_declarative_pipeline` function is a custom function built for this course to create the sample pipeline using the Databricks REST API. This avoids manually creating the pipeline and referencing the pipeline assets.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC create_declarative_pipeline(pipeline_name=f'6 - Change Data Capture with AUTO CDC - {catalog_name}', 
-- MAGIC                             root_path_folder_name='6 - Change Data Capture with AUTO CDC Project',
-- MAGIC                             catalog_name = catalog_name,
-- MAGIC                             schema_name = 'default',
-- MAGIC                             source_folder_names=['orders', 'status', 'customers'],
-- MAGIC                             configuration = {'source':working_dir})

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Complete the following steps to open the starter Spark Declarative Pipeline project for this demonstration:
-- MAGIC
-- MAGIC    a. In the main navigation bar right-click on **Jobs & Pipelines** and select **Open in Link in New Tab**.
-- MAGIC
-- MAGIC    b. In **Jobs & Pipelines** select your **6 - Change Data Capture with AUTO CDC - labuser** pipeline.
-- MAGIC
-- MAGIC    c. **REQUIRED:** At the top near your pipeline name, turn on **New pipeline monitoring**.
-- MAGIC
-- MAGIC    d. In the **Pipeline details** pane on the far right, select **Open in Editor** (field to the right of **Source code**) to open the pipeline in the **Lakeflow Pipeline Editor**.
-- MAGIC
-- MAGIC    e. In the new tab you should see five folders: 
-- MAGIC       - **explorations**
-- MAGIC       - **orders**
-- MAGIC       - **status**
-- MAGIC       - **customers**
-- MAGIC       - Plus the extra **python_excluded** folder that contains the Python version. 
-- MAGIC
-- MAGIC    f. Open the **customers** folder and select the **customers_pipeline.sql** file.
-- MAGIC       - **NOTE:** The **status** and **orders** pipelines are the same as we saw in the previous demonstrations.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## D. Spark Declarative Pipeline CDC SCD Type 1 Pipeline Steps
-- MAGIC Follow the steps below using the **customers_pipeline.sql** file in the Lakeflow Pipelines editor.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### PLEASE COMPLETE FIRST: Click the 'Run Pipeline' button to execute the Pipeline
-- MAGIC 1. To save some time, let's run the entire pipeline for **status**, **orders** and **customers**. While the pipeline is running explore the code in the **customers_pipeline.sql** for the new customers flow.
-- MAGIC
-- MAGIC ##### While the pipeline is running continue through the steps below to review the customer pipeline code.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### STEP 1: JSON -> Bronze Ingestion
-- MAGIC The code in **STEP 1** of the **customers_pipeline.sql** file:
-- MAGIC    - We define a bronze streaming table named **customers_bronze_raw_demo6** using a data source configured with Auto Loader (`FROM STREAM`).
-- MAGIC    - Adds the table property `pipelines.reset.allowed = false` to prevent deletion of all ingested bronze data if a full refresh is triggered.
-- MAGIC    - Creates columns to capture the time of data ingestion and the source file name for each row.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### STEP 2: Create the Bronze Clean Streaming Table with Data Quality Enforcement
-- MAGIC ##### **NOTE:** This displays how you can use advanced data quality techniques with expectations. Advanced expectations are outside the scope of this course.
-- MAGIC
-- MAGIC The code in **STEP 2** of the **customers_pipeline.sql** file:
-- MAGIC
-- MAGIC - Adds three violation constraint actions: **WARN**, **DROP**, and **FAIL**. Each defines how to handle constraint violations.
-- MAGIC - Applies multiple conditions to a single constraint.
-- MAGIC - Uses a built-in SQL function within a constraint.
-- MAGIC
-- MAGIC #### About the data source:
-- MAGIC
-- MAGIC - The data is a CDC feed that contains **`INSERT`**, **`UPDATE`**, and **`DELETE`** operations for customers.  
-- MAGIC - REQUIREMENT: **UPDATE** and **INSERT** operations should contain valid entries for all fields.  
-- MAGIC - REQUIREMENT: **DELETE** operations should contain **`NULL`** values for all fields except the **timestamp**, **customer_id**, and **operation** fields.
-- MAGIC
-- MAGIC **NOTE:** To ensure only valid data reaches our silver table, we'll write a series of quality enforcement rules that allow expected null values in **DELETE** operations while rejecting bad data elsewhere.
-- MAGIC
-- MAGIC
-- MAGIC ### We'll break down each of these constraints below:
-- MAGIC
-- MAGIC ##### 1. **`valid_id`**
-- MAGIC This constraint will cause our transaction to fail if a record contains a null value in the **`customer_id`** field.
-- MAGIC
-- MAGIC ##### 2. **`valid_operation`**
-- MAGIC This constraint will drop any records that contain a null value in the **`operation`** field.
-- MAGIC
-- MAGIC ##### 3. **`valid_name`**
-- MAGIC This constraint will track any records that contain a null value in the **`name`** field. Because there is no additional instruction for what to do with invalid records, violating rows will be recorded in metrics but not dropped.
-- MAGIC
-- MAGIC ##### 4. **`valid_address`**
-- MAGIC This constraint checks if the **`operation`** field is **`DELETE`**; if not, it checks for null values in any of the 4 fields comprising an address. Because there is no additional instruction for what to do with invalid records, violating rows will be recorded in metrics but not dropped.
-- MAGIC
-- MAGIC ##### 5. **`valid_email`**
-- MAGIC This constraint uses regex pattern matching to check that the value in the **`email`** field is a valid email address. It contains logic to not apply this to records if the **`operation`** field is **`DELETE`** (because these will have a null value for the **`email`** field). Violating records are dropped.
-- MAGIC
-- MAGIC **NOTE:** When a record is going to be dropped, all values except the **customer_id** will be `null`.
-- MAGIC | address                               | city         | customer_id | email                    | name           | operation | state |
-- MAGIC |---------------------------------------|--------------|-------------|--------------------------|----------------|-----------|-------|
-- MAGIC | null                                  | null         | 23617       | null                     | null           | DELETE    | null  |
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### STEP 3: Processing CDC Data with **`AUTO CDC INTO`**
-- MAGIC Spark Declarative Pipelines introduces a new syntactic structure for simplifying CDC feed processing: `AUTO CDC INTO` (formerly `APPLY CHANGES INTO`).
-- MAGIC
-- MAGIC The code in **STEP 3** of the **customers_pipeline.sql** file uses `AUTO CDC INTO` to:
-- MAGIC - Create the **2_silver_db.scd_type_1_customers_silver_demo6** streaming table if it doesn't exist,
-- MAGIC - Updates the **2_silver_db.scd_type_1_customers_silver_demo6** streaming table with updates, inserts and deletes using records from the **1_bronze_db.customers_bronze_clean_demo6** streaming table.
-- MAGIC
-- MAGIC #### Additional Notes
-- MAGIC **`AUTO CDC INTO`** has the following guarantees and requirements:
-- MAGIC - Performs incremental/streaming ingestion of CDC data
-- MAGIC - Provides simple syntax to specify one or many fields as the primary key for a table
-- MAGIC - Default assumption is that rows will contain inserts and updates
-- MAGIC - Can optionally apply deletes
-- MAGIC - Automatically orders late-arriving records using user-provided sequencing key (order to process rows)
-- MAGIC - Uses a simple syntax for specifying columns to ignore with the **`EXCEPT`** keyword
-- MAGIC - The default to applying changes is SCD Type 1. You can also use SCD Type 2 if you would like. We will focus on SCD Type 1.
-- MAGIC
-- MAGIC
-- MAGIC #### Documentation
-- MAGIC [AUTO CDC INTO (Lakeflow Spark Declarative Pipelines)](https://docs.databricks.com/aws/en/dlt-ref/dlt-sql-ref-apply-changes-into)
-- MAGIC
-- MAGIC [The AUTO CDC APIs: Simplify change data capture with Lakeflow Spark Declarative Pipelines](https://docs.databricks.com/aws/en/dlt/cdc)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### STEP 4: Explore the Customers Pipeline Graph
-- MAGIC After running the pipeline and reviewing the code cells, take time to explore the pipeline results for the **customers** flow following the steps below.
-- MAGIC
-- MAGIC **Run with 1 JSON File**
-- MAGIC
-- MAGIC ![demo6_cdc_run01.png](./Includes/images/demo6_cdc_run_1.png)
-- MAGIC
-- MAGIC <br></br>
-- MAGIC Notice the following:
-- MAGIC 1. In the **customers** flow in the pipeline graph, notice that **939** rows were streamed into the three streaming tables. 
-- MAGIC     - This is because all records are new and valid entries, they were ingested throughout the flow.
-- MAGIC
-- MAGIC 2. In the table window below, find the **scd1_type_1_customers_silver_demo06** table and select **Table metrics**. Note the following:
-- MAGIC
-- MAGIC     - The **Upserted** column indicates that all **939** rows were upserted into the table, as all rows are new.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### STEP 5: Explore the Customers Pipeline Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Run the query below to view the **scd_type_1_customers_silver_demo6** streaming table (the table with SCD Type 1 updates, inserts and deletes). 
-- MAGIC
-- MAGIC     Notice the following after the first run ingestion the **00.json** file:
-- MAGIC
-- MAGIC    - The streaming table contains all **939 rows** from the **00.json** file, since they are all new customers being added to the target table.
-- MAGIC
-- MAGIC    - Each record was inserted into the empty streaming table.

-- COMMAND ----------

SELECT *
FROM 2_silver_db.scd_type_1_customers_silver_demo6;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Query the **scd_type_1_customers_silver_demo6** streaming table for the following **customer_id** values (*23225*, *23617*). 
-- MAGIC
-- MAGIC    Notice the following:
-- MAGIC       - **customer_id** = *23225*
-- MAGIC          - **Address**: `76814 Jacqueline Mountains Suite 815`  
-- MAGIC          - **State**: `TX`  
-- MAGIC       - **customer_id** = *23617*
-- MAGIC          - This customer exists in the first execution (in file **00.json**)

-- COMMAND ----------

SELECT *
FROM 2_silver_db.scd_type_1_customers_silver_demo6
WHERE customer_id IN (23225, 23617);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## E. Land New Data to Your Data Source Volume
-- MAGIC Complete the following after executing and reviewing the **customers** pipeline flow that consistent of ingesting one file (**00.json**) from cloud storage.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Run the cell below to land a new JSON file to each volume (**customers**, **status** and **orders**) to simulate new files being added to your cloud storage locations.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC copy_file_for_multiple_sources(copy_n_files = 2, 
-- MAGIC                                sleep_set = 1,
-- MAGIC                                copy_from_source='/Volumes/databricks_simulated_retail_customer_data/v01/retail-pipeline',
-- MAGIC                                copy_to_target = working_dir)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Run the cell below to programmatically view the files in your `/Volumes/dbacademy/ops/labuser-name/customers` volume. Confirm your volume now contains the original **00.json** file and the new **01.json** file.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f'LIST "{working_dir}/customers"').display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Run the cell to explore the raw data in the new **01.json** file prior to ingesting it in your pipeline. 
-- MAGIC
-- MAGIC    Notice the following:
-- MAGIC
-- MAGIC    - This file contains **23** rows.
-- MAGIC
-- MAGIC    - The **operation** column specifies **UPDATE**, **DELETE**, and **NEW** operations for customers.
-- MAGIC       - **In the new 01.json file there are**:
-- MAGIC          - 12 customers with **UPDATE** values
-- MAGIC          - 1 customer with a **DELETE** value
-- MAGIC          - 10 new customers with a **NEW** value
-- MAGIC
-- MAGIC    - In the results below, find the row with **customer_id** *23225* and note the following:
-- MAGIC
-- MAGIC       - The original address for **Sandy Adams** (from the streaming table, file **00.json**) was: `76814 Jacqueline Mountains Suite 815`, `TX`
-- MAGIC       - The updated address for **Sandy Adams** (from the file below) is: `512 John Stravenue Suite 239`, `TN`
-- MAGIC
-- MAGIC    - In the results below, find the row with **customer_id** *23617* and note the following:
-- MAGIC       - The **operation** for this customer is **DELETE**.
-- MAGIC       - When the **operation** column is delete, all other column values are `null`.

-- COMMAND ----------

SELECT *
FROM read_files(
  '/Volumes/pipeline/pipeline_data/data' || '/customers/01.json',
  format => "JSON"
)
ORDER BY customer_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### E1. Go back to your pipeline and click **'Run pipeline'** button to ingest the new JSON file (**01.json**) incrementally and perform CDC SCD Type 1 on the **scd_type1_customers_silver_demo06** table.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## F. Explore the Customers Pipeline
-- MAGIC
-- MAGIC After you have explored and landed 1 new JSON file into each of your cloud data sources, complete the following to explore the **customers** flow in the **Pipeline graph**:
-- MAGIC
-- MAGIC a. 23 rows were read into the: 
-- MAGIC
-- MAGIC   - **customers_bronze_raw_demo06** streaming table
-- MAGIC   - **customers_bronze_clean_demo06** streaming table (all data quality checks passed)
-- MAGIC   - The pipeline only ingested and processed the NEW **01.json** file 
-- MAGIC
-- MAGIC b. In the **scd_type_1_customers_silver_demo6** streaming table details (The CDC SCD Type 1 table) it contains:
-- MAGIC   - **Upserted = 22**:
-- MAGIC     - 12 customers with UPDATE values (previous customer were simply updated with the new values)
-- MAGIC     - 10 new customers with a NEW value (new customers were inserted into the table)
-- MAGIC   - **Deleted records = 1**:
-- MAGIC     - 1 customer was marked as DELETE and deleted from the table
-- MAGIC
-- MAGIC ![Run 2](./Includes/images/demo6_cdc_run_2.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## G. Explore the CDC SCD Type 1 on the scd_type_1_customers_silver_demo6 Streaming Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. View the data in the **scd_type_1_customers_silver_demo6** streaming table with SCD Type 1 and observe the following:
-- MAGIC
-- MAGIC    a. The table contains **948 rows**:
-- MAGIC       - **initial 939 customers** 
-- MAGIC       - \+ **10** new customers
-- MAGIC       - \- **1** deleted customer
-- MAGIC       - **NOTES:** 
-- MAGIC          - The **12** updates to original customers were made in place and updated the original record (SCD Type 1 does not keep historical records).
-- MAGIC          - The **1** record marked for deletion was deleted from the table.

-- COMMAND ----------

SELECT customer_id, address, name
FROM 2_silver_db.scd_type_1_customers_silver_demo6;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Query the **2_silver_db.scd_type_1_customers_silver_demo6** table for the following **customer_id** values: *23225* and *23617*. These were the values we reviewed earlier.  
-- MAGIC
-- MAGIC     Notice the following:  
-- MAGIC
-- MAGIC     - **customer_id** *23225* has been updated to the new address. The historical address was not retained because we used SCD Type 1.  
-- MAGIC     - **customer_id** *23617* has been deleted from the table. It no longer exists because we used SCD Type 1.  
-- MAGIC

-- COMMAND ----------

SELECT *
FROM 2_silver_db.scd_type_1_customers_silver_demo6
WHERE customer_id IN (23225, 23617);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Additional Resources
-- MAGIC
-- MAGIC - [What is change data capture (CDC)?](https://docs.databricks.com/aws/en/dlt/what-is-change-data-capture)
-- MAGIC
-- MAGIC - [AUTO CDC INTO (Lakeflow Spark Declarative Pipelines)](https://docs.databricks.com/gcp/en/dlt-ref/dlt-sql-ref-apply-changes-into) documentation
-- MAGIC
-- MAGIC - [The AUTO CDC APIs: Simplify change data capture with Lakeflow Spark Declarative Pipelines](https://docs.databricks.com/aws/en/dlt/cdc) documentation
-- MAGIC
-- MAGIC - [How to implement Slowly Changing Dimensions when you have duplicates - Part 1: What to look out for?](https://community.databricks.com/t5/technical-blog/how-to-implement-slowly-changing-dimensions-when-you-have/ba-p/40568)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
