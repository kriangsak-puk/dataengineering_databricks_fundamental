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
-- MAGIC # 5 - Deploying a Pipeline to Production
-- MAGIC
-- MAGIC In this demonstration, we will begin by adding an additional data source to our pipeline and performing a join with our streaming tables. Then, we will focus on productionalizing the pipeline by adding comments and table properties to the objects we create, scheduling the pipeline, and creating an event log to monitor the pipeline.
-- MAGIC
-- MAGIC ### Learning Objectives
-- MAGIC
-- MAGIC By the end of this lesson, you will be able to:
-- MAGIC - Apply the appropriate comment syntax and table properties to pipeline objects to enhance readability.
-- MAGIC - Demonstrate how to perform a join between two streaming tables using a materialized view to optimize data processing.
-- MAGIC - Execute the scheduling of a pipeline using trigger or continuous modes to ensure timely processing.
-- MAGIC - Explore the event log to monitor a production Lakeflow Spark Declarative Pipeline.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Setup
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
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
-- MAGIC # @DBAcademyHelper.add_method
-- MAGIC # def create_catalogs(self, catalog_suffix: list):
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
-- MAGIC

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
-- MAGIC ## B. Explore the Orders and Status JSON Files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Explore the raw data located in the `/Volumes/dbacademy/ops/our-lab-user/orders/` volume. This is the data we have been working with throughout the course demonstrations.
-- MAGIC
-- MAGIC    Run the cell below to view the results. Notice that the orders JSON file(s) contains information about when each order was placed.

-- COMMAND ----------

SELECT *
FROM read_files(
  '/Volumes/pipeline/pipeline_data/data' || '/orders/',
  format => 'JSON'
)
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Explore the **status** raw data located in the `/Volumes/dbacademy/ops/your-lab-user/status/` volume and filter for the specific **order_id** *75123*.
-- MAGIC
-- MAGIC    Run the cell below to view the results. Notice that the status JSON file(s) contain **order_status** information for each order.  
-- MAGIC
-- MAGIC    **NOTE:** The **order_status** can include multiple rows per order and may be any of the following:
-- MAGIC
-- MAGIC    - on the way  
-- MAGIC    - canceled  
-- MAGIC    - return canceled  
-- MAGIC    - reported shipping error  
-- MAGIC    - delivered  
-- MAGIC    - return processed  
-- MAGIC    - return picked up  
-- MAGIC    - placed  
-- MAGIC    - preparing  
-- MAGIC    - return requested
-- MAGIC

-- COMMAND ----------

SELECT *
FROM read_files(
  '/Volumes/pipeline/pipeline_data/data' || '/status/',
  format => 'JSON'
)
WHERE order_id = 75123;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. One of our objectives is to join the **orders** data with the order **status** data.  
-- MAGIC
-- MAGIC     The query below demonstrates what the result of the final join in the Spark Declarative Pipeline will look like after the data has been incrementally ingested and cleaned when we create the pipeline. Run the cell and review the output.
-- MAGIC
-- MAGIC     Notice that after joining the tables, we can see each **order_id** along with its original **order_timestamp** and the **order_status** at specific points in time.
-- MAGIC
-- MAGIC **NOTE:** The data used in this demo is artificially generated, so the **order_status_timestamps** may not reflect realistic timing.

-- COMMAND ----------

WITH orders AS (
  SELECT *
  FROM read_files(
        '/Volumes/pipeline/pipeline_data/data' || '/orders/',
        format => 'JSON'
  )
),
status AS (
  SELECT *
  FROM read_files(
        '/Volumes/pipeline/pipeline_data/data' || '/status/',
        format => 'JSON'
  )
)
-- Join the views to get the order history with status
SELECT
  orders.order_id,
  timestamp(orders.order_timestamp) AS order_timestamp,
  status.order_status,
  timestamp(status.status_timestamp) AS order_status_timestamp
FROM orders
  INNER JOIN status 
  ON orders.order_id = status.order_id
ORDER BY order_id, order_status_timestamp;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C. Putting a Pipeline in Production
-- MAGIC
-- MAGIC This course includes a complete Lakeflow Spark Declarative Pipeline project that has already been created.  In this section, you'll explore the Spark Declarative Pipeline and modify its settings for production use.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. The screenshot below shows what the final Spark Declarative Pipeline will look like when ingesting a single JSON file from the data sources:  
-- MAGIC ![Final Demo 6 Pipeline](./Includes/images/demo5_pipeline_image_run1.png)
-- MAGIC
-- MAGIC     **Note:** Depending on the number of files you've ingested, the row count may vary.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Run the cell below to create your starter Spark Declarative Pipeline for this demonstration. The pipeline will set the following for you:
-- MAGIC     - Your default catalog: `labuser`
-- MAGIC     - Your configuration parameter: `source` = `/Volumes/dbacademy/ops/your-labuser-name`
-- MAGIC
-- MAGIC     **NOTE:** If the pipeline already exists, an error will be returned. In that case, you'll need to delete the existing pipeline and rerun this cell.
-- MAGIC
-- MAGIC     To delete the pipeline:
-- MAGIC
-- MAGIC     a. Select **Jobs & Pipelines** from the far-left navigation bar.  
-- MAGIC
-- MAGIC     b. Find the pipeline you want to delete.  
-- MAGIC
-- MAGIC     c. Click the three-dot menu ![ellipsis icon](./Includes/images/ellipsis_icon.png).  
-- MAGIC
-- MAGIC     d. Select **Delete**.
-- MAGIC
-- MAGIC **NOTE:**  The `create_declarative_pipeline` function is a custom function built for this course to create the sample pipeline using the Databricks REST API. This avoids manually creating the pipeline and referencing the pipeline assets.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC create_declarative_pipeline(pipeline_name=f'5 - Deploying a Pipeline to Production Project - {catalog_name}', 
-- MAGIC                             root_path_folder_name='5 - Deploying a Pipeline to Production Project',
-- MAGIC                             catalog_name = catalog_name,
-- MAGIC                             schema_name = 'default',
-- MAGIC                             source_folder_names=['orders', 'status'],
-- MAGIC                             configuration = {'source':working_dir})

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Complete the following steps to open the starter Spark Declarative Pipeline project for this demonstration:
-- MAGIC
-- MAGIC    a. In the main navigation bar right-click on **Jobs & Pipelines** and select **Open in Link in New Tab**.
-- MAGIC
-- MAGIC    b. In **Jobs & Pipelines** select your **5 - Deploying a Pipeline to Production Project - labuser** pipeline.
-- MAGIC
-- MAGIC    c. **REQUIRED:** At the top near your pipeline name, turn on **New pipeline monitoring**.
-- MAGIC
-- MAGIC    d. In the **Pipeline details** pane on the far right, select **Open in Editor** (field to the right of **Source code**) to open the pipeline in the **Lakeflow Pipeline Editor**.
-- MAGIC
-- MAGIC    e. In the new tab you should see three folders: **explorations**, **orders**, and **status** (plus the extra **python_excluded** folder that contains the Python version). 
-- MAGIC
-- MAGIC    f. Continue to step 4 and 5 below.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Explore the code in the `orders/orders_pipeline.sql` file
-- MAGIC
-- MAGIC 4. In the new tab select the **orders** folder. It contains the same **orders_pipeline.sql** pipeline you've been working with.  **Follow the instructional comments in the file to proceed.**
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Explore the code in the `status/status_pipeline` notebook
-- MAGIC
-- MAGIC 5. After reviewing the **orders_pipeline.sql** file, you'll be directed to explore the **status/status_pipeline.sql** notebook. This notebook processes new data and adds it to the pipeline. **Follow the instructions provided in the notebook's markdown cells.**
-- MAGIC
-- MAGIC     **NOTE:** The **status/status_pipeline.sql**  notebook will go through setting up the pipeline settings, scheduling and running the production pipeline.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## D. Land More Data to Your Data Source Volume

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Run the cell below to add **4** more JSON files to your volumes:
-- MAGIC     - `/Volumes/dbacademy/ops/your-labuser-volume/orders`
-- MAGIC     - `/Volumes/dbacademy/ops/your-labuser-volume/status`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC copy_files(copy_from = '/Volumes/databricks_simulated_retail_customer_data/v01/retail-pipeline/orders/stream_json/', 
-- MAGIC            copy_to = f'{working_dir}/orders', 
-- MAGIC            n = 5)
-- MAGIC
-- MAGIC copy_files(copy_from = '/Volumes/databricks_simulated_retail_customer_data/v01/retail-pipeline/status/stream_json/', 
-- MAGIC            copy_to = f'{working_dir}/status', 
-- MAGIC            n = 5)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Navigate back to your pipeline and select **Run pipeline** to process the new landed files.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## E. Introduction to the Pipeline Event Log (Advanced Topic)
-- MAGIC
-- MAGIC After running your pipeline and successfully publishing the event log as a table named **event_log_demo_5** in your **labuser.default** schema (database), begin exploring the event log. 
-- MAGIC
-- MAGIC Here we will quickly introduce the event log. **To process the event log you will need knowledge of parsing JSON formatted strings.**
-- MAGIC
-- MAGIC   - [Monitor Lakeflow Spark Declarative Pipelines](https://docs.databricks.com/aws/en/dlt/observability) documentation
-- MAGIC
-- MAGIC **TROUBLESHOOT:** 
-- MAGIC - **REQUIRED:** If you did not run the pipeline and publish the event log, the code below will not run. Please make sure to complete all steps before starting this section.
-- MAGIC
-- MAGIC - **HIDDEN EVENT LOG:** By default, Spark Declarative Pipelines writes the event log to a hidden Delta table in the default catalog and schema configured for the pipeline. While hidden, the table can still be queried by all sufficiently privileged users. By default, only the owner of the pipeline can query the event log table. By default, the name for the hidden event log is formatted as:  
-- MAGIC   - `catalog.schema.event_log_{pipeline_id}` - where the pipeline ID is the system-assigned UUID with dashes replaced by underscores.  
-- MAGIC   - [Query the Event Log](https://docs.databricks.com/aws/en/dlt/observability#query-the-event-log)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Complete the following steps to view the **labuser.default.event_log_demo_5** event log in your catalog:
-- MAGIC
-- MAGIC    a. Select the catalog icon ![Catalog Icon](./Includes/images/catalog_icon.png) from the left navigation pane.
-- MAGIC
-- MAGIC    b. Expand your **labuser** catalog.
-- MAGIC
-- MAGIC    c. Expand the following schemas (databases):
-- MAGIC       - **1_bronze_db**
-- MAGIC       - **2_silver_db**
-- MAGIC       - **3_gold_db**
-- MAGIC       - **default**
-- MAGIC
-- MAGIC    d. Notice the following:
-- MAGIC       - In the **1_bronze_db**, **2_silver_db**, and **3_gold_db** schemas, the pipeline streaming tables and materialized views were created (they end with **demo5**).
-- MAGIC       - In the **default** schema, the pipeline has published the event log as a table named **event_log_demo_5**.
-- MAGIC
-- MAGIC **NOTE:** You might need to refresh the catalogs to view the streaming tables, materialized views, and event log.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Query your **labuser.default.event_log_demo_5** table to see what the event log looks like.
-- MAGIC
-- MAGIC    Notice that it contains all events within the pipeline as **STRING** columns (typically JSON-formatted strings) or **STRUCT** columns. Databricks supports the `:` (colon) operator to parse JSON fields. See the [`:` operator documentation](https://docs.databricks.com/) for more details.
-- MAGIC
-- MAGIC    The following table describes the event log schema. Some fields contain JSON data—such as the **details** field—which must be parsed to perform certain queries.

-- COMMAND ----------

SELECT *
FROM pipeline.default.event_log_demo_5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC | Field          | Description |
-- MAGIC |----------------|-------------|
-- MAGIC | `id`           | A unique identifier for the event log record. |
-- MAGIC | `sequence`     | A JSON document containing metadata to identify and order events. |
-- MAGIC | `origin`       | A JSON document containing metadata for the origin of the event, for example, the cloud provider, the cloud provider region, user_id, pipeline_id, or pipeline_type to show where the pipeline was created, either DBSQL or WORKSPACE. |
-- MAGIC | `timestamp`    | The time the event was recorded. |
-- MAGIC | `message`      | A human-readable message describing the event. |
-- MAGIC | `level`        | The event type, for example, INFO, WARN, ERROR, or METRICS. |
-- MAGIC | `maturity_level` | The stability of the event schema. The possible values are:<br><br>- **STABLE**: The schema is stable and will not change.<br>- **NULL**: The schema is stable and will not change. The value may be NULL if the record was created before the maturity_level field was added (release 2022.37).<br>- **EVOLVING**: The schema is not stable and may change.<br>- **DEPRECATED**: The schema is deprecated and the pipeline runtime may stop producing this event at any time. |
-- MAGIC | `error`        | If an error occurred, details describing the error. |
-- MAGIC | `details`      | A JSON document containing structured details of the event. This is the primary field used for analyzing events. |
-- MAGIC | `event_type`   | The event type. |
-- MAGIC
-- MAGIC **[Event Log Schema](https://docs.databricks.com/aws/en/ldp/monitor-event-log-schema)**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. The majority of the detailed information you will want from the event log is located in the **details** column, which is a JSON-formatted string. You will need to parse this column.
-- MAGIC
-- MAGIC    You can find more information in the Databricks documentation on how to [query JSON strings](https://docs.databricks.com/aws/en/semi-structured/json).
-- MAGIC
-- MAGIC    The code below will:
-- MAGIC
-- MAGIC    - Return the **event_type** column.
-- MAGIC
-- MAGIC    - Return the entire **details** JSON-formatted string.
-- MAGIC
-- MAGIC    - Parse out the **flow_progress** values from the **details** JSON-formatted string, if they exist.
-- MAGIC
-- MAGIC    - Parse out the **user_action** values from the **details** JSON-formatted string, if they exist.
-- MAGIC

-- COMMAND ----------

SELECT
  id,
  event_type,
  details,
  details:flow_progress,
  details:user_action
FROM pipeline.default.event_log_demo_5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. One use case for the event log is to examine data quality metrics for all runs of your pipeline. These metrics provide valuable insights into your pipeline, both in the short term and long term. Metrics are captured for each constraint throughout the entire lifetime of the table.
-- MAGIC
-- MAGIC    Below is an example query to obtain those metrics. We won’t dive into the JSON parsing code here. This example simply demonstrates what’s possible with the **event_log**.
-- MAGIC
-- MAGIC    Run the cell and observe the results. Notice the following:
-- MAGIC    - The **passing_records** for each constraint are displayed.
-- MAGIC    - The **failing_records** (WARN) for each constraint are displayed.
-- MAGIC
-- MAGIC **NOTE:** If you have selected **Run pipeline with full table refresh** at any time during your pipeline, your results will include metrics from previous runs as well as from the full refresh. Additional logic is required to isolate results after the full table refresh. This is outside the scope of this course.
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW dq_source_vw AS
SELECT explode(
            from_json(details:flow_progress:data_quality:expectations,
                      "array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>")
          ) AS row_expectations
   FROM pipeline.default.event_log_demo_5
   WHERE event_type = 'flow_progress';


-- View the data
SELECT 
  row_expectations.dataset as dataset,
  row_expectations.name as expectation,
  SUM(row_expectations.passed_records) as passing_records,
  SUM(row_expectations.failed_records) as warnings_records
FROM dq_source_vw
GROUP BY row_expectations.dataset, row_expectations.name
ORDER BY dataset;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Summary
-- MAGIC
-- MAGIC This was a quick introduction to the pipeline **event_log**. With the **event_log**, you can investigate all aspects of your pipeline runs to explore the runs as well as create overall reports. Feel free to investigate the **event_log** further on your own.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Additional Resources
-- MAGIC
-- MAGIC - [Lakeflow Spark Declarative Pipelines properties reference](https://docs.databricks.com/aws/en/dlt/properties#dlt-table-properties)
-- MAGIC
-- MAGIC - [Table properties and table options](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-tblproperties)
-- MAGIC
-- MAGIC - [Triggered vs. continuous pipeline mode](https://docs.databricks.com/aws/en/dlt/pipeline-mode)
-- MAGIC
-- MAGIC - [Development and production modes](https://docs.databricks.com/aws/en/dlt/updates#development-and-production-modes)
-- MAGIC
-- MAGIC - [Monitor Lakeflow Spark Declarative Pipelines](https://docs.databricks.com/aws/en/dlt/observability)
-- MAGIC
-- MAGIC - **Materialized views include built-in optimizations where applicable:**
-- MAGIC   - [Incremental refresh for materialized views](https://docs.databricks.com/aws/en/optimizations/incremental-refresh)
-- MAGIC   - [Delta Live Tables Announces New Capabilities and Performance Optimizations](https://www.databricks.com/blog/2022/06/29/delta-live-tables-announces-new-capabilities-and-performance-optimizations.html)
-- MAGIC   - [Cost-effective, incremental ETL with serverless compute for Delta Live Tables pipelines](https://www.databricks.com/blog/cost-effective-incremental-etl-serverless-compute-delta-live-tables-pipelines)
-- MAGIC
-- MAGIC - **Stateful joins:** For stateful joins in pipelines (i.e., joining incrementally as data is ingested), refer to the [Optimize stateful processing in Lakeflow Spark Declarative Pipelines with watermarks](https://docs.databricks.com/aws/en/dlt/stateful-processing) documentation. **Stateful joins are an advanced topic and outside the scope of this course.**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
