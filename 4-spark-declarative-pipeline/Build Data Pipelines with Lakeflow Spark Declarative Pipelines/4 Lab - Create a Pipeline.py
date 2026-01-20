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
# MAGIC # 4 Lab - Create a Pipeline  
# MAGIC ### Estimated Duration: ~15-20 minutes
# MAGIC
# MAGIC In this lab, you'll migrate a traditional ETL workflow to a pipeline for incremental data processing. You'll practice building streaming tables and materialized views using Lakeflow Spark Declarative Pipelines syntax.
# MAGIC
# MAGIC #### Your Tasks:
# MAGIC - Create a new Pipeline  
# MAGIC - Convert traditional SQL ETL to declarative syntax for incremental processing 
# MAGIC - Configure pipeline settings  
# MAGIC - Define data quality expectations  
# MAGIC - Validate and run the pipeline
# MAGIC
# MAGIC ### Learning Objectives
# MAGIC
# MAGIC By the end of this lesson, you will be able to:
# MAGIC - Create a pipeline and execute it successfully using the new Lakeflow Pipeline Editor.
# MAGIC - Modify and configure pipeline settings to align with specific data processing requirements.
# MAGIC - Integrate data quality expectations into a pipeline and evaluate their effectiveness.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC

# COMMAND ----------

import json
import os
import json
from databricks.sdk import WorkspaceClient


def create_declarative_pipeline(pipeline_name: str, 
                        root_path_folder_name: str,
                        source_folder_names: list = [],
                        catalog_name: str = 'dbacademy',
                        schema_name: str = 'default',
                        serverless: bool = True,
                        configuration: dict = {},
                        continuous: bool = False,
                        photon: bool = True,
                        channel: str = 'PREVIEW',
                        development: bool = True,
                        pipeline_type = 'WORKSPACE'
                        ):
  
    '''
  Creates the specified DLT pipeline.

  Parameters:
  ----------
  pipeline_name : str
      The name of the DLT pipeline to be created.
  root_path_folder_name : str
      The root folder name where the pipeline will be located. This folder must be in the location where this function is called.
  source_folder_names : list, optional
      A list of source folder names. Must defined at least one folder within the root folder location above.
  catalog_name : str, optional
      The catalog name for the DLT pipeline. Default is 'dbacademy'.
  schema_name : str, optional
      The schema name for the DLT pipeline. Default is 'default'.
  serverless : bool, optional
      If True, the pipeline will be serverless. Default is True.
  configuration : dict, optional
      A dictionary of configuration settings for the pipeline. Default is an empty dictionary.
  continuous : bool, optional
      If True, the pipeline will be run in continuous mode. Default is False.
  photon : bool, optional
      If True, the pipeline will use Photon for processing. Default is True.
  channel : str, optional
      The channel for the pipeline, such as 'PREVIEW'. Default is 'PREVIEW'.
  development : bool, optional
      If True, the pipeline will be set up for development. Default is True.
  pipeline_type : str, optional
      The type of the pipeline (e.g., 'WORKSPACE'). Default is 'WORKSPACE'.

  Returns:
  -------
  None
      This function does not return anything. It creates the DLT pipeline based on the provided parameters.

  Example:
  --------
  create_dlt_pipeline(pipeline_name='my_pipeline_name', 
                      root_path_folder_name='6 - Putting a DLT Pipeline in Production Project',
                      source_folder_names=['orders', 'status'])
  '''
  
    w = WorkspaceClient()
    for pipeline in w.pipelines.list_pipelines():
        if pipeline.name == pipeline_name:
            raise ValueError(f"Lakeflow Declarative Pipeline name '{pipeline_name}' already exists. Please delete the pipeline using the UI and rerun the cell to recreate the pipeline.")

    ## Create empty dictionary
    create_dlt_pipeline_call = {}

    ## Pipeline type
    create_dlt_pipeline_call['pipeline_type'] = pipeline_type

    ## Modify dictionary for specific DLT configurations
    create_dlt_pipeline_call['name'] = pipeline_name

    ## Set paths to root and source folders
    main_course_folder_path = os.getcwd()

    main_path_to_dlt_project_folder = os.path.join('/', main_course_folder_path, root_path_folder_name)
    create_dlt_pipeline_call['root_path'] = main_path_to_dlt_project_folder

    ## Add path of root folder to source folder names
    add_path_to_folder_names = [os.path.join(main_path_to_dlt_project_folder, folder_name, '**') for folder_name in source_folder_names]
    source_folders_path = [{'glob':{'include':folder_name}} for folder_name in add_path_to_folder_names]
    create_dlt_pipeline_call['libraries'] = source_folders_path

    ## Set default catalog and schema
    create_dlt_pipeline_call['catalog'] = catalog_name
    create_dlt_pipeline_call['schema'] = schema_name

    ## Set serverless compute
    create_dlt_pipeline_call['serverless'] = serverless

    ## Set configuration parameters
    create_dlt_pipeline_call['configuration'] = configuration

    ## Set if continouous or not
    create_dlt_pipeline_call['continuous'] = continuous 

    ## Set to use Photon
    create_dlt_pipeline_call['photon'] = photon

    ## Set DLT channel
    create_dlt_pipeline_call['channel'] = channel

    ## Set if development mode
    create_dlt_pipeline_call['development'] = development

    ## Creat DLT pipeline

    print(f"Creating the Lakeflow Declarative Pipeline '{pipeline_name}'...")
    print(f"Root folder path: {main_path_to_dlt_project_folder}")
    print(f"Source folder path(s): {source_folders_path}")

    w.api_client.do('POST', '/api/2.0/pipelines', body=create_dlt_pipeline_call)
    print(f"\nLakeflow Declarative Pipeline Creation '{pipeline_name}' Complete!")

# COMMAND ----------

def create_country_lookup_table(in_catalog: str, in_schema: str):
    # List of country abbreviations and names
    country_data = [
        ('AF', 'Afghanistan'),
        ('AL', 'Albania'),
        ('DZ', 'Algeria'),
        ('AS', 'American Samoa'),
        ('AD', 'Andorra'),
        ('AO', 'Angola'),
        ('AI', 'Anguilla'),
        ('AQ', 'Antarctica'),
        ('AR', 'Argentina'),
        ('AM', 'Armenia'),
        ('AW', 'Aruba'),
        ('AU', 'Australia'),
        ('AT', 'Austria'),
        ('AZ', 'Azerbaijan'),
        ('BS', 'Bahamas'),
        ('BH', 'Bahrain'),
        ('BD', 'Bangladesh'),
        ('BB', 'Barbados'),
        ('BY', 'Belarus'),
        ('BE', 'Belgium'),
        ('BZ', 'Belize'),
        ('BJ', 'Benin'),
        ('BM', 'Bermuda'),
        ('BT', 'Bhutan'),
        ('BO', 'Bolivia'),
        ('BA', 'Bosnia and Herzegovina'),
        ('BW', 'Botswana'),
        ('BR', 'Brazil'),
        ('BN', 'Brunei Darussalam'),
        ('BG', 'Bulgaria'),
        ('BF', 'Burkina Faso'),
        ('BI', 'Burundi'),
        ('KH', 'Cambodia'),
        ('CM', 'Cameroon'),
        ('CA', 'Canada'),
        ('CV', 'Cape Verde'),
        ('KY', 'Cayman Islands'),
        ('CF', 'Central African Republic'),
        ('TD', 'Chad'),
        ('CL', 'Chile'),
        ('CN', 'China'),
        ('CO', 'Colombia'),
        ('KM', 'Comoros'),
        ('CG', 'Congo'),
        ('CD', 'Democratic Republic of the Congo'),
        ('CK', 'Cook Islands'),
        ('CR', 'Costa Rica'),
        ('CI', 'Côte d\'Ivoire'),
        ('HR', 'Croatia'),
        ('CU', 'Cuba'),
        ('CY', 'Cyprus'),
        ('CZ', 'Czech Republic'),
        ('DK', 'Denmark'),
        ('DJ', 'Djibouti'),
        ('DM', 'Dominica'),
        ('DO', 'Dominican Republic'),
        ('EC', 'Ecuador'),
        ('EG', 'Egypt'),
        ('SV', 'El Salvador'),
        ('GQ', 'Equatorial Guinea'),
        ('ER', 'Eritrea'),
        ('EE', 'Estonia'),
        ('ET', 'Ethiopia'),
        ('FK', 'Falkland Islands'),
        ('FO', 'Faroe Islands'),
        ('FJ', 'Fiji'),
        ('FI', 'Finland'),
        ('FR', 'France'),
        ('GA', 'Gabon'),
        ('GM', 'Gambia'),
        ('GE', 'Georgia'),
        ('DE', 'Germany'),
        ('GH', 'Ghana'),
        ('GI', 'Gibraltar'),
        ('GR', 'Greece'),
        ('GL', 'Greenland'),
        ('GD', 'Grenada'),
        ('GP', 'Guadeloupe'),
        ('GU', 'Guam'),
        ('GT', 'Guatemala'),
        ('GN', 'Guinea'),
        ('GW', 'Guinea-Bissau'),
        ('GY', 'Guyana'),
        ('HT', 'Haiti'),
        ('HM', 'Heard Island and McDonald Islands'),
        ('HN', 'Honduras'),
        ('HK', 'Hong Kong'),
        ('HU', 'Hungary'),
        ('IS', 'Iceland'),
        ('IN', 'India'),
        ('ID', 'Indonesia'),
        ('IR', 'Iran'),
        ('IQ', 'Iraq'),
        ('IE', 'Ireland'),
        ('IL', 'Israel'),
        ('IT', 'Italy'),
        ('JM', 'Jamaica'),
        ('JP', 'Japan'),
        ('JO', 'Jordan'),
        ('KZ', 'Kazakhstan'),
        ('KE', 'Kenya'),
        ('KI', 'Kiribati'),
        ('KP', 'North Korea'),
        ('KR', 'South Korea'),
        ('KW', 'Kuwait'),
        ('KG', 'Kyrgyzstan'),
        ('LA', 'Laos'),
        ('LV', 'Latvia'),
        ('LB', 'Lebanon'),
        ('LS', 'Lesotho'),
        ('LR', 'Liberia'),
        ('LY', 'Libya'),
        ('LI', 'Liechtenstein'),
        ('LT', 'Lithuania'),
        ('LU', 'Luxembourg'),
        ('MO', 'Macao'),
        ('MK', 'North Macedonia'),
        ('MG', 'Madagascar'),
        ('MW', 'Malawi'),
        ('MY', 'Malaysia'),
        ('MV', 'Maldives'),
        ('ML', 'Mali'),
        ('MT', 'Malta'),
        ('MH', 'Marshall Islands'),
        ('MQ', 'Martinique'),
        ('MR', 'Mauritania'),
        ('MU', 'Mauritius'),
        ('YT', 'Mayotte'),
        ('MX', 'Mexico'),
        ('FM', 'Federated States of Micronesia'),
        ('MD', 'Moldova'),
        ('MC', 'Monaco'),
        ('MN', 'Mongolia'),
        ('ME', 'Montenegro'),
        ('MS', 'Montserrat'),
        ('MA', 'Morocco'),
        ('MZ', 'Mozambique'),
        ('MM', 'Myanmar (Burma)'),
        ('NA', 'Namibia'),
        ('NR', 'Nauru'),
        ('NP', 'Nepal'),
        ('NL', 'Netherlands'),
        ('NC', 'New Caledonia'),
        ('NZ', 'New Zealand'),
        ('NI', 'Nicaragua'),
        ('NE', 'Niger'),
        ('NG', 'Nigeria'),
        ('NU', 'Niue'),
        ('NF', 'Norfolk Island'),
        ('MP', 'Northern Mariana Islands'),
        ('NO', 'Norway'),
        ('OM', 'Oman'),
        ('PK', 'Pakistan'),
        ('PW', 'Palau'),
        ('PS', 'Palestine'),
        ('PA', 'Panama'),
        ('PG', 'Papua New Guinea'),
        ('PY', 'Paraguay'),
        ('PE', 'Peru'),
        ('PH', 'Philippines'),
        ('PN', 'Pitcairn Islands'),
        ('PL', 'Poland'),
        ('PT', 'Portugal'),
        ('PR', 'Puerto Rico'),
        ('QA', 'Qatar'),
        ('RE', 'Réunion'),
        ('RO', 'Romania'),
        ('RU', 'Russia'),
        ('RW', 'Rwanda'),
        ('SA', 'Saudi Arabia'),
        ('SN', 'Senegal'),
        ('RS', 'Serbia'),
        ('SC', 'Seychelles'),
        ('SL', 'Sierra Leone'),
        ('SG', 'Singapore'),
        ('SX', 'Sint Maarten'),
        ('SK', 'Slovakia'),
        ('SI', 'Slovenia'),
        ('SB', 'Solomon Islands'),
        ('SO', 'Somalia'),
        ('ZA', 'South Africa'),
        ('SS', 'South Sudan'),
        ('ES', 'Spain'),
        ('LK', 'Sri Lanka'),
        ('SD', 'Sudan'),
        ('SR', 'Suriname'),
        ('SJ', 'Svalbard and Jan Mayen'),
        ('SZ', 'Swaziland'),
        ('SE', 'Sweden'),
        ('CH', 'Switzerland'),
        ('SY', 'Syria'),
        ('TW', 'Taiwan'),
        ('TJ', 'Tajikistan'),
        ('TZ', 'Tanzania'),
        ('TH', 'Thailand'),
        ('TL', 'Timor-Leste'),
        ('TG', 'Togo'),
        ('TK', 'Tokelau'),
        ('TO', 'Tonga'),
        ('TT', 'Trinidad and Tobago'),
        ('TN', 'Tunisia'),
        ('TR', 'Turkey'),
        ('TM', 'Turkmenistan'),
        ('TC', 'Turks and Caicos Islands'),
        ('TV', 'Tuvalu'),
        ('UG', 'Uganda'),
        ('UA', 'Ukraine'),
        ('AE', 'United Arab Emirates'),
        ('GB', 'United Kingdom'),
        ('US', 'United States'),
        ('UY', 'Uruguay'),
        ('UZ', 'Uzbekistan'),
        ('VU', 'Vanuatu'),
        ('VA', 'Vatican City'),
        ('VE', 'Venezuela'),
        ('VN', 'Vietnam'),
        ('WF', 'Wallis and Futuna'),
        ('YE', 'Yemen'),
        ('ZM', 'Zambia'),
        ('ZW', 'Zimbabwe')
    ]

    # Convert the list into a Spark DataFrame
    df_country = spark.createDataFrame(country_data, ['country_abbreviation', 'country_name'])

    # Show the DataFrame
    df_country.write.mode("overwrite").saveAsTable(f"{in_catalog}.{in_schema}.country_lookup")

# COMMAND ----------

import os  
import pandas as pd
from io import StringIO

class LabDataSetup:
    """
    Sets up lab data by checking for the existence of a catalog, schema, volume and creating the CSV files in a staging volume.

    - Catalog, schema and volume must already exist.

    Example:
      obj = LabDataSetup('dbacademy_peter_s','default','lab_staging_files')
    """

    def __init__(self, catalog_name: str, schema_name: str, volume_name: str):
        self.catalog_name = catalog_name
        self.schema_name = schema_name
        self.volume_name = volume_name
        self.volume_path = os.path.join('/Volumes', self.catalog_name, self.schema_name, self.volume_name)

        print("Starting environment validation...")
        self._validate_environment()

        dict_of_files = {
            'employees_1.csv': self.create_csv_1_data(),
            'employees_2.csv': self.create_csv_2_data(),
            'employees_3.csv': self.create_csv_3_data()
        }

        for filename, filefunc in dict_of_files.items():
            self.create_csv_file_if_not_exists(file_name=filename, csv_data_func=filefunc)

        print(f"LabDataSetup initialized successfully in volume_path: '{self.volume_path}'")

    def _validate_environment(self):
        self._validate_catalog()
        self._validate_schema()
        self._validate_volume()

    def _validate_catalog(self):
        catalogs = spark.sql("SHOW CATALOGS").collect()
        catalog_names = [row.catalog for row in catalogs]
        if self.catalog_name in catalog_names:
            print(f"Catalog '{self.catalog_name}' exists.")
        else:
            print(f"Catalog '{self.catalog_name}' does not exist.")
            raise FileNotFoundError(f"{self.catalog_name} catalog not found.")

    def _validate_schema(self):
        full_schema_name = f"{self.catalog_name}.{self.schema_name}"
        if spark.catalog.databaseExists(full_schema_name):
            print(f"Schema '{full_schema_name}' exists.")
        else:
            print(f"Schema '{full_schema_name}' does not exist.")
            raise FileNotFoundError(f"{full_schema_name} schema not found.")

    def _validate_volume(self):
        volumes = spark.sql(f"SHOW VOLUMES IN {self.catalog_name}.{self.schema_name}").collect()
        volume_names = [v.volume_name for v in volumes]
        if self.volume_name in volume_names:
            print(f"Volume '{self.volume_name}' exists.")
        else:
            print(f"Volume '{self.volume_name}' does not exist.")
            raise FileNotFoundError(f"{self.volume_name} volume not found.")

    def check_if_file_exists(self, file_name: str) -> bool:
        file_path = os.path.join(self.volume_path, file_name)
        if os.path.exists(file_path):
            print(f"The file '{file_path}' exists.")
            return True
        else:
            print(f"The file '{file_path}' does not exist.")
            return False

    def create_csv_file(self, csv_string: str, file_to_create: str):
        df = pd.read_csv(StringIO(csv_string))
        output_path = os.path.join(self.volume_path, file_to_create)
        df.to_csv(output_path, index=False)
        print(f"Created CSV file at '{output_path}'.")

    def create_csv_file_if_not_exists(self, file_name: str, csv_data_func: callable):
        if not self.check_if_file_exists(file_name=file_name):
            print(f"Creating file '{file_name}'...")
            self.create_csv_file(csv_string=csv_data_func, file_to_create=file_name)
        else:
            print(f"File '{file_name}' already exists. Skipping creation.")

    def create_csv_1_data(self) -> str:
        return """EmployeeID,FirstName,Country,Department,Salary,HireDate,Operation,ProcessDate
null,test,test,test,9999,2025-01-01,new,2025-06-05
1,Sophia,US,Sales,72000,2025-04-01,new,2025-06-05
2,Nikos,Gr,IT,55000,2025-04-10,new,2025-06-05
3,Liam,US,Sales,69000,2025-05-03,new,2025-06-05
4,Elena,GR,IT,53000,2025-06-04,new,2025-06-05
5,James,Us,IT,60000,2025-06-05,new,2025-06-05"""

    def create_csv_2_data(self) -> str:
        return """EmployeeID,FirstName,Country,Department,Salary,HireDate,Operation,ProcessDate
6,Emily,us,Enablement,80000,2025-06-09,new,2025-06-22
7,Yannis,gR,HR,70000,2025-06-20,new,2025-06-22
3,Liam,US,Sales,100000,2025-05-03,update,2025-06-22
1,,,,,,delete,2025-06-22"""

    def create_csv_3_data(self):
        return """EmployeeID,FirstName,Country,Department,Salary,HireDate,Operation,ProcessDate
8,Panagiotis,Gr,Enablement,90000,2025-07-01,new,2025-07-22
6,,,,,,delete,2025-07-22
2,,,,,,delete,2025-07-22"""

    def copy_file(self, copy_file: str, to_target_volume: str):
        dbutils.fs.cp(f'{self.volume_path}/{copy_file}', f'{to_target_volume}/{copy_file}')
        print(f"Moving file '{self.volume_path}/{copy_file}' to '{to_target_volume}/{copy_file}'.")

    def delete_lab_staging_files(self):
        dbutils.fs.rm(self.volume_path, True)
        print(f"Deleted all files in '{self.volume_path}'.")

    def __str__(self):
        return f"LabDataSetup(catalog_name={self.catalog_name}, schema_name={self.schema_name}, volume_name={self.volume_name}, volume_path={self.volume_path})"

# COMMAND ----------

def create_volume(in_catalog: str, in_schema: str, volume_name: str):
    '''
    Create a volume in the specified catalog.schema.
    '''
    print(f'Creating volume: {in_catalog}.{in_schema}.{volume_name} if not exists.\n')
    r = spark.sql(f'CREATE VOLUME IF NOT EXISTS {in_catalog}.{in_schema}.{volume_name}')
# @DBAcademyHelper.add_method
# def create_catalogs(self, catalog_suffix: list):
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


# COMMAND ----------

catalog_name = "pipeline"
schema_name = ['pipeline_data', '1_bronze_db', '2_silver_db', '3_gold_db']
volume_name = "data"
data_volume_path = f"/Volumes/{catalog_name}/{schema_name[0]}/{volume_name}"
working_dir = data_volume_path
print(working_dir)

## Create volume for the lab
create_volume(in_catalog=catalog_name, in_schema = 'default', volume_name = 'lab_staging_files')
create_volume(in_catalog=catalog_name, in_schema = 'default', volume_name = 'lab_files')

## Create schemas for lab data
create_schemas(in_catalog = catalog_name, schema_names = ['lab_1_bronze_db', 'lab_2_silver_db', 'lab_3_gold_db'])


## Create the country_lookup table if it doesn't exist
if spark.catalog.tableExists(f"{catalog_name}.default.country_lookup") == False:
    create_country_lookup_table(in_catalog = catalog_name, in_schema = 'default')
else:
    print(f'Table {catalog_name}.default.country_lookup already exists. No action taken')

delete_source_files(f'/Volumes/{catalog_name}/default/lab_files/')
delete_source_files(f'/Volumes/{catalog_name}/default/lab_files_staging/')

# Example usage
LabSetup = LabDataSetup(f'{catalog_name}','default','lab_staging_files')
LabSetup.copy_file(copy_file = 'employees_1.csv', 
                   to_target_volume = f'/Volumes/{catalog_name}/default/lab_files')

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. SCENARIO
# MAGIC
# MAGIC Your data engineering team has identified an opportunity to modernize an existing ETL pipeline that was originally developed in a Databricks notebook. While the current pipeline gets the job done, it lacks the scalability, observability, efficiency and automated data quality features required as your data volume and complexity grow.
# MAGIC
# MAGIC To address this, you've been asked to migrate the existing pipeline to a Lakeflow Spark Declarative Pipeline. Spark Declarative Pipelines will enable your team to define data transformations more declaratively, apply data quality rules, and benefit from built-in optimization, lineage tracking and monitoring.
# MAGIC
# MAGIC Your goal is to refactor the original notebook based logic (shown in the cells below) into a Spark Declarative Pipeline.
# MAGIC
# MAGIC ### REQUIREMENTS:
# MAGIC   - Migrate the ETL code below to a Spark Declarative Pipeline.
# MAGIC   - Add the required data quality expectations to the bronze table and silver table:
# MAGIC   - Create materialized views for the most up to date aggregated information.
# MAGIC
# MAGIC Follow the steps below to complete your task.

# COMMAND ----------

# MAGIC %md
# MAGIC ### B1. Explore the Raw Data
# MAGIC
# MAGIC 1. Complete the following steps to view where our lab's streaming raw source files are coming from:
# MAGIC
# MAGIC    a. Select the **Catalog** icon ![Catalog Icon](./Includes/images/catalog_icon.png) in the left navigation bar.  
# MAGIC
# MAGIC    b. Expand your **pipeline** catalog.  
# MAGIC
# MAGIC    c. Expand the **default** schema.  
# MAGIC
# MAGIC    d. Expand **Volumes**.  
# MAGIC
# MAGIC    e. Expand the **lab_files** volume.  
# MAGIC
# MAGIC    f. You should see a single CSV file named **employees_1.csv**. If not, refresh the catalogs.  
# MAGIC
# MAGIC    g. The files in the **lab_files** volume will be the data source files you will be ingesting.

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Run the cell below to view the raw CSV file in your **lab_files** volume. Notice the following:
# MAGIC
# MAGIC    - It’s a simple CSV file separated by commas.  
# MAGIC    - It contains headers.  
# MAGIC    - It has 7 rows in total (6 data records and 1 header row).  
# MAGIC    - The first record (row 2) is a test record and should not be included in the pipeline and will be dropped by a data quality expectation.

# COMMAND ----------

spark.sql(f'''
        SELECT *
        FROM csv.`/Volumes/pipeline/default/lab_files/`
        ''').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### B2. Current ETL Code
# MAGIC
# MAGIC Run each cell below to view the results of the current ETL pipeline. This will give you an idea of the expected output. Don’t worry too much about the data transformations within the SQL queries.
# MAGIC
# MAGIC The focus of this lab is on using **declarative SQL** and creating a **Spark Declarative Pipeline**. You will not need to modify the transformation logic, only the `CREATE` statements and `FROM` clauses to ensure data is read and processed incrementally in your pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC #### B2.1 - CSV to Bronze
# MAGIC
# MAGIC Explore the code and run the cell. Observe the results. Notice that:
# MAGIC
# MAGIC - The CSV file in the volume is read in as a table named **employees_bronze_lab4** in the **pipeline.lab_1_bronze_db** schema.  
# MAGIC - The table contains 6 rows with the correct column names.
# MAGIC
# MAGIC Think about what you will need to change when migrating this to a Spake Declarative Pipeline. Hints are added as comments in the code below.
# MAGIC
# MAGIC **NOTE:** In your Spark Declarative Pipeline we will want to add data quality expectations to document any bad data coming into the pipeline.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Specify to use your pipeline catalog
# MAGIC USE CATALOG IDENTIFIER("pipeline");
# MAGIC
# MAGIC
# MAGIC CREATE OR REPLACE STREAMING TABLE lab_1_bronze_db.employees_bronze_lab4  -- You will have to modify this to create a streaming table in the pipeline
# MAGIC AS
# MAGIC SELECT 
# MAGIC   *,
# MAGIC   current_timestamp() AS ingestion_time,
# MAGIC   _metadata.file_name as raw_file_name
# MAGIC FROM read_files(                                           -- You will have to modify FROM clause to incrementally read in data
# MAGIC   '/Volumes/' || "pipeline" || '/default/lab_files',  -- You will have to modify this path in the pipeline to your specific raw data source
# MAGIC   format => 'CSV',
# MAGIC   header => 'true'
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- Display table
# MAGIC SELECT *
# MAGIC FROM lab_1_bronze_db.employees_bronze_lab4;

# COMMAND ----------

# MAGIC %md
# MAGIC #### B2.2 - Bronze to Silver
# MAGIC
# MAGIC Run the cell below to create the table **pipeline.lab_2_silver_db.employees_silver_lab4** and explore the results. Notice that a few simple data transformations were applied to the bronze table, and metadata columns were removed.
# MAGIC
# MAGIC Think about what you will need to change when migrating this to a Spark Declarative Pipeline. Hints are added as comments in the code below.
# MAGIC
# MAGIC **NOTE:** For simplicity, we are leaving the **test** row in place, and you will remove it using data quality expectations. Typically, we could have just filtered out the null value(s).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lab_2_silver_db.employees_silver_lab4 -- You will have to modify this to create a streaming table in the pipeline
# MAGIC AS
# MAGIC SELECT
# MAGIC   EmployeeID,
# MAGIC   FirstName,
# MAGIC   upper(Country) AS Country,
# MAGIC   Department,
# MAGIC   Salary,
# MAGIC   HireDate,
# MAGIC   date_format(HireDate, 'MMMM') AS HireMonthName,
# MAGIC   year(HireDate) AS HireYear, 
# MAGIC   Operation
# MAGIC FROM lab_1_bronze_db.employees_bronze_lab4;                    -- You will have to modify FROM clause to incrementally read in data
# MAGIC
# MAGIC
# MAGIC -- Display table
# MAGIC SELECT *
# MAGIC FROM lab_2_silver_db.employees_silver_lab4;

# COMMAND ----------

# MAGIC %md
# MAGIC #### B2.3 - Silver to Gold
# MAGIC The code below creates two traditional views to aggregate the silver tables.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Run the cell to create a view that calculates the total number of employees and total salary by country.
# MAGIC
# MAGIC     Think about what you will need to change when migrating this to a Spark Declarative Pipeline. A hint is added as a comment in the code below.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE MATERIALIZED VIEW lab_3_gold_db.employees_by_country_gold_lab4 -- You will have to modify this to create a materialized view in the pipeline
# MAGIC AS
# MAGIC SELECT 
# MAGIC   Country,
# MAGIC   count(*) AS TotalEmployees,
# MAGIC   sum(Salary) AS TotalSalary
# MAGIC FROM lab_2_silver_db.employees_silver_lab4
# MAGIC GROUP BY Country;
# MAGIC
# MAGIC
# MAGIC -- Display view
# MAGIC SELECT *
# MAGIC FROM lab_3_gold_db.employees_by_country_gold_lab4;

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Run the cell to create a view that calculates the salary by department.
# MAGIC
# MAGIC     Think about what you will need to change when migrating this to a Spark Declarative Pipeline. A hint is added as a comment in the code below.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW lab_3_gold_db.salary_by_department_gold_lab4  -- You will have to modify this to create a materialized view in the pipeline
# MAGIC AS
# MAGIC SELECT
# MAGIC   Department,
# MAGIC   sum(Salary) AS TotalSalary
# MAGIC FROM lab_2_silver_db.employees_silver_lab4
# MAGIC GROUP BY Department;
# MAGIC
# MAGIC
# MAGIC -- Display view
# MAGIC SELECT *
# MAGIC FROM lab_3_gold_db.salary_by_department_gold_lab4;

# COMMAND ----------

# MAGIC %md
# MAGIC #### B2.4 - Delete the Tables
# MAGIC
# MAGIC Run the cell below to delete all the tables you created above. You will recreate them as streaming tables and materialized views in the Spark Declarative Pipeline.
# MAGIC
# MAGIC **NOTE:** If you have created the streaming tables and materialized views with Spark Declarative Pipelines and want to drop them to redo this lab, the following code will not work with the lab's default **No isolation shared cluster**. You will have to run the cells in this notebook using Serverless or manually delete the pipeline and tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS lab_1_bronze_db.employees_bronze_lab4;
# MAGIC DROP TABLE IF EXISTS lab_2_silver_db.employees_silver_lab4;
# MAGIC DROP VIEW IF EXISTS lab_3_gold_db.employees_by_country_gold_lab4;
# MAGIC DROP VIEW IF EXISTS lab_3_gold_db.salary_by_department_gold_lab4;

# COMMAND ----------

# MAGIC %md
# MAGIC Run the cell below to view and copy the path to your **lab_files** volume. You will need this path when building your pipeline to reference your data source files.
# MAGIC
# MAGIC **NOTE:** You can also navigate to the volume and copy the path using the UI.

# COMMAND ----------

print(f'/Volumes/{catalog_name}/default/lab_files')

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. TO DO: Create the Lakeflow Spark Declarative Pipeline (Steps)
# MAGIC
# MAGIC After you have explored the traditional ETL code to create the tables and views, it's time to modify that syntax to declarative SQL for your new pipeline.
# MAGIC
# MAGIC You will have to complete the following:
# MAGIC
# MAGIC **NOTE:** The solution files can be found in the **4 - Lab Solution Project**. All code is in the one **ingest.sql** file:
# MAGIC
# MAGIC 1. Create a Spark Declarative Pipeline and name it **Lab4 - firstname pipeline project**.
# MAGIC
# MAGIC     - Select your **pipeline** catalog  
# MAGIC
# MAGIC     - Select the **default** schema  
# MAGIC
# MAGIC     - Select the **Start with sample code in SQL** language  
# MAGIC
# MAGIC     - **NOTE:** The Spark Declarative Pipeline will contain sample files and notebooks. You can exclude the sample files from the pipeline before you run the pipeline.
# MAGIC
# MAGIC
# MAGIC 2. Migrate the ETL code (shown below for each step as markdown) into one or more files and folders to organize your pipeline (you can also put everything in a single file if you want).
# MAGIC <br></br>
# MAGIC ##### 2a. Modify the code (shown below) to create the **bronze** streaming table by completing the following:
# MAGIC
# MAGIC ```SQL
# MAGIC CREATE OR REPLACE TABLE lab_1_bronze_db.employees_bronze_lab4  -- You will have to modify this to create a streaming table in the pipeline
# MAGIC AS
# MAGIC SELECT 
# MAGIC     *,
# MAGIC     current_timestamp() AS ingestion_time,
# MAGIC     _metadata.file_name as raw_file_name
# MAGIC FROM read_files(                                           -- You will have to modify FROM clause to incrementally read in data
# MAGIC     '/Volumes/' || catalog_name || '/default/lab_files',  -- You will have to modify this path in the pipeline to your specific raw data source
# MAGIC     format => 'CSV',
# MAGIC     header => 'true'
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC - Modify the `CREATE OR REPLACE TABLE` statement to create a streaming table.  
# MAGIC
# MAGIC - Add the keyword `STREAM` in the `FROM` clause to incrementally ingest data from the delta table.
# MAGIC
# MAGIC - Modify the path in the `FROM` clause to point to your **labuser.default.lab_files** volume path (example: `/Volumes/labuser1234/default/lab_files`). You can statically add the path in the `read_files` function, or use a configuration parameter.
# MAGIC
# MAGIC <br></br>
# MAGIC ##### 2b. Modify the code (shown below) to create the **silver** streaming table by completing the following in your pipeline project:
# MAGIC
# MAGIC ```
# MAGIC CREATE OR REPLACE TABLE lab_2_silver_db.employees_silver_lab4 -- You will have to modify this to create a streaming table in the pipeline
# MAGIC AS
# MAGIC SELECT
# MAGIC     EmployeeID,
# MAGIC     FirstName,
# MAGIC     upper(Country) AS Country,
# MAGIC     Department,
# MAGIC     Salary,
# MAGIC     HireDate,
# MAGIC     date_format(HireDate, 'MMMM') AS HireMonthName,
# MAGIC     year(HireDate) AS HireYear, 
# MAGIC     Operation
# MAGIC FROM lab_1_bronze_db.employees_bronze_lab4;                    -- You will have to modify FROM clause to incrementally read in data
# MAGIC ```
# MAGIC
# MAGIC - Modify the `CREATE OR REPLACE TABLE` statement to create a streaming table.  
# MAGIC
# MAGIC - Add the keyword `STREAM` in the `FROM` clause to incrementally ingest data.
# MAGIC
# MAGIC - Add the following data quality expectations:      
# MAGIC ```
# MAGIC CONSTRAINT check_country EXPECT (Country IN ('US','GR')),
# MAGIC CONSTRAINT check_salary EXPECT (Salary > 0),
# MAGIC CONSTRAINT check_null_id EXPECT (EmployeeID IS NOT NULL) ON VIOLATION DROP ROW
# MAGIC
# MAGIC ```
# MAGIC <br></br>
# MAGIC ##### 2c. Replace the `CREATE OR REPLACE VIEW` statement in the two views to create materialized views instead of traditional views in your pipeline project.
# MAGIC
# MAGIC ```
# MAGIC CREATE OR REPLACE VIEW lab_3_gold_db.employees_by_country_gold_lab4 -- You will have to modify this to create a materialized view in the pipeline
# MAGIC AS
# MAGIC SELECT 
# MAGIC     Country,
# MAGIC     count(*) AS TotalEmployees,
# MAGIC     sum(Salary) AS TotalSalary
# MAGIC FROM lab_2_silver_db.employees_silver_lab4
# MAGIC GROUP BY Country;
# MAGIC
# MAGIC
# MAGIC CREATE OR REPLACE VIEW lab_3_gold_db.salary_by_department_gold_lab4  -- You will have to modify this to create a materialized view in the pipeline
# MAGIC AS
# MAGIC SELECT
# MAGIC     Department,
# MAGIC     sum(Salary) AS TotalSalary
# MAGIC FROM lab_2_silver_db.employees_silver_lab4
# MAGIC GROUP BY Department;
# MAGIC
# MAGIC ```
# MAGIC <br></br>
# MAGIC 3. Pipeline configuration requirements:
# MAGIC
# MAGIC - Your Spark Declarative Pipeline should use **Serverless** compute.  
# MAGIC
# MAGIC - Your pipeline should use your **labuser** catalog by default.  
# MAGIC
# MAGIC - Your pipeline should use your **default** schema by default.
# MAGIC
# MAGIC - Make sure your pipeline is including your .sql file only.
# MAGIC
# MAGIC - (OPTIONAL) If using a configuration variable to point to your path make sure it is setup and applied in the `read_files` function.
# MAGIC
# MAGIC <br></br>
# MAGIC 4. When complete, run the pipeline. Troubleshoot any errors.
# MAGIC
# MAGIC <br></br>
# MAGIC
# MAGIC ##### Final Spark Declarative Pipeline Image  
# MAGIC Below is what your final pipeline should look like after the first run with a single CSV file.
# MAGIC
# MAGIC ![Final Lab4 Pipeline](./Includes/images/lab4_solutiongraph.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### LAB SOLUTION (optional)
# MAGIC If you need the solution, you can view the lab solution code in the **4 - Lab Solution Project** folder. You can also execute the code below to automatically create the Spark Declarative Pipeline with the necessary configuration settings for your specific lab.
# MAGIC
# MAGIC **NOTE:** After you run the cell, wait 30 seconds for the pipeline to finish creating. Then open one of the files in the **4 - Lab Solution Project** folder to open the new Spark Declarative Pipeline editor.

# COMMAND ----------

create_declarative_pipeline(pipeline_name=f'4 - Lab Solution Project - {catalog_name}', 
                            root_path_folder_name='4 - Lab Solution Project',
                            catalog_name = catalog_name,
                            schema_name = 'default',
                            source_folder_names=['solution'],
                            configuration = {'source':f'/Volumes/{catalog_name}/default/lab_files'})

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Explore the Streaming Tables and Materialized Views
# MAGIC
# MAGIC After you have created and run your Spark Declarative Pipeline, complete the following tasks to explore your new streaming tables and materialized views.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. In the catalog explorer on the left, expand your **pipeline** catalog and expand the following schemas:
# MAGIC    - **lab_1_bronze_db**
# MAGIC    - **lab_2_silver_db**
# MAGIC    - **lab_3_gold_db**
# MAGIC
# MAGIC     You should see the two streaming tables and materialized views within your schemas (if you don't use the solution, you won't have the **_solution** at the end of the streaming tables and materialized views):
# MAGIC
# MAGIC    <img src="./Includes/images/lab4_solution_schemas.png" alt="Objects in Schemas" width="350">
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Run the cell below to view the data in your **pipeline.lab_1_bronze_db.employees_bronze_lab4** streaming table. Notice that the first row contains a `null` **EmployeeID**.
# MAGIC
# MAGIC **NOTE:** If you ran the solution pipeline, the streaming table is named **employees_bronze_lab4_solution**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM lab_1_bronze_db.employees_bronze_lab4;

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Run the cell below to view the data in your **pipeline.lab_2_silver_db.employees_silver_lab4** streaming table. Notice that the silver table removed the **EmployeeID** value that contained a `null` using a data quality expectation.
# MAGIC
# MAGIC **NOTE:** If you ran the solution pipeline, the streaming table is named **employees_silver_lab4_solution**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM lab_2_silver_db.employees_silver_lab4;

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Run the cell below to view the data in your **pipeline.lab_3_gold_db.employees_by_country_gold_lab4** materialized view. 
# MAGIC
# MAGIC     **Final Results**
# MAGIC     | Country | TotalCount | TotalSalary |
# MAGIC     |---------|------------|-------------|
# MAGIC     | GR      | 2          | 108000      |
# MAGIC     | US      | 3          | 201000      |
# MAGIC
# MAGIC **NOTE:** If you ran the solution pipeline, the materialized view is named **employees_by_country_gold_lab4_solution**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM lab_3_gold_db.employees_by_country_gold_lab4
# MAGIC ORDER BY TotalSalary;

# COMMAND ----------

# MAGIC %md
# MAGIC 5. Run the cell below to view the data in your **pipeline.lab_3_gold_db.salary_by_department_gold_lab4** materialized view. 
# MAGIC
# MAGIC     **Final Results**
# MAGIC     | Department  | TotalSalary |
# MAGIC     |-------------|-------------|
# MAGIC     | Sales          | 141000      |
# MAGIC     | IT          | 168000      |
# MAGIC
# MAGIC **NOTE:** If you ran the solution pipeline, the materialized view is named **salary_by_department_gold_lab4_solution**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM lab_3_gold_db.salary_by_department_gold_lab4
# MAGIC ORDER BY TotalSalary;

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. CHALLENGE SCENARIO (OPTIONAL IN LIVE TEACH)
# MAGIC ### Duration: ~10 minutes
# MAGIC
# MAGIC **NOTE:** *If you finish early in a live class, feel free to complete the challenge below. The challenge is optional and most likely won't be completed during the live class. Only continue if your Spark Declarative Pipeline was set up correctly in the previous section by comparing your pipeline to the solution image.*
# MAGIC
# MAGIC **SCENARIO:** In the challenge, you will land a new CSV file in your **lab_files** cloud storage volume and rerun the pipeline to watch the Spark Declarative Pipeline only ingest the new data.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Run the cell below to copy another file to your **labuser.default.lab_user** volume.
# MAGIC

# COMMAND ----------

LabSetup.copy_file(copy_file = 'employees_2.csv', 
                   to_target_volume = f'/Volumes/{catalog_name}/default/lab_files')

# COMMAND ----------

# MAGIC %md
# MAGIC 2. In the left navigation area, navigate to your **labuser.default.lab_files** volume and expand the volume. Confirm it contains two CSV files: 
# MAGIC     - **employees_1.csv** 
# MAGIC     - **employees_2.csv**
# MAGIC
# MAGIC **NOTE:** You might have to refresh your catalogs if the file is not shown.

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Run the cell below to preview only the new CSV file and view the results. Notice that the new CSV file contains employee information:
# MAGIC
# MAGIC     - Contains 4 rows.  
# MAGIC     - The **Operation** column specifies an action for each employee (e.g., update the record, delete the record, or add a new employee).
# MAGIC
# MAGIC **NOTE:** Don’t worry about the **Operation** column yet. We’ll cover how to capture these specific changes in your data (Change Data Capture) in a later demonstration.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM read_files(                                           
# MAGIC   '/Volumes/' || 'pipeline' || '/default/lab_files/employees_2.csv',  
# MAGIC   format => 'CSV',
# MAGIC   header => 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Now that you have explored the new CSV file in cloud storage, go back to your Spark Declarative Pipeline and select **Run pipeline**. Notice that the pipeline only read in the new file in cloud storage.
# MAGIC
# MAGIC
# MAGIC ##### Final Spark Declarative Pipeline Image
# MAGIC Below is what your final pipeline should look like after the first run with a single CSV file.
# MAGIC
# MAGIC ![Final Challenge Lab4 DLT Pipeline](./Includes/images/lab4_solutionchallenge.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 5. Explore the history of your streaming tables using the Catalog Explorer. Notice that there are two appends to both the **bronze** and **silver** tables.

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
