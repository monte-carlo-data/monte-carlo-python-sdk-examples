# Databricks notebook source
# MAGIC %md # Instructions
# MAGIC ## What is this?
# MAGIC This notebook will download data exports from Monte Carlo using the Monte Carlo API and then load them into Delta Table(s). Each data export will be loaded to its own Delta Table. This script will create / replace the Delta Table each time it is run. The table names will be "mcd_export_exportname"
# MAGIC
# MAGIC ## Prerequisites
# MAGIC * Through the Monte Carlo UI create an API token.
# MAGIC * Store the Token ID and Token Value in a DBX Secret key repo named 'monte-carlo-creds' with the keys 'mcd-id' and 'mcd-token'
# MAGIC     * Alternatively you can set the ID and Token in this notebook direclty by editing the cell of this notebook named 'Find/Set API Credentials'
# MAGIC * This script will not create a _schema_ for you. It is assumed that the schema you provide already exists.
# MAGIC
# MAGIC ## Running the notebook
# MAGIC * After the 'Create User Input Widgets' command is run, there will be two drop down widgets at the top of the notebook
# MAGIC   * EXPORTS TO DOWNLOAD: Lets you select which data export(s) you want to download. The default will be ALL. If you want to only download a set of specific exports, de-select ALL and select the exports you want.
# MAGIC   * SCHEMA TO WRITE TO: The schema under which the Delta Tables will be created/replaced.
# MAGIC * Run the rest of the commands to download the data exports from Monte Carlo and import them to Databricks

# COMMAND ----------

# MAGIC %md # Environment Setup

# COMMAND ----------

#Install the Monte Carlo Python Library (Notebook scoped)
#More info here: https://docs.databricks.com/libraries/notebooks-python-libraries.html#install-a-library-with-pip
%pip install pycarlo

# COMMAND ----------

# DBTITLE 1,Find/Set API Credentials
# Monte Carlo Credentials stored in DBX Secret Key Repo called "monte-carlo-creds":
mcd_id = dbutils.secrets.get(scope="monte-carlo-creds", key="mcd-id")
mcd_token = dbutils.secrets.get(scope="monte-carlo-creds", key="mcd-token")

# Other variables which you can customize:
mcd_profile = ""

# COMMAND ----------

# DBTITLE 1,Define Available Data Exports
from pycarlo.core import Client, Query, Session

client = Client(session=Session(mcd_id=mcd_id, mcd_token=mcd_token, mcd_profile=mcd_profile))

# Available data exports (these are the standard Monte Carlo data exports)
data_export_names = ['MONITORS', 'ALERTS', 'EVENTS', 'ASSETS']
data_export_report_names = ['monitors.csv', 'alerts.csv', 'events.csv', 'assets.csv']

# Create mapping for easier processing
export_name_to_report_mapping = dict(zip(data_export_names, data_export_report_names))

# COMMAND ----------

# DBTITLE 1,Create User Input Widgets
dbutils.widgets.multiselect(
    'EXPORTS TO DOWNLOAD',
    defaultValue='ALL',
    choices=['ALL'] + data_export_names
)
dbutils.widgets.text("SCHEMA TO WRITE TO", "mcd_exports")

# COMMAND ----------

# DBTITLE 1,Runtime Variables (Pulled From Input Widgets)
export_names = dbutils.widgets.get("EXPORTS TO DOWNLOAD").split(',')

# If ALL is in list of export_names selected, even if other individual exports are selected, we will download all exports
if export_names == ['ALL']:
    export_report_names = [(export, export_name_to_report_mapping[export]) for export in data_export_names]
elif 'ALL' in export_names:
    raise Exception("De-select 'ALL' from Exports to Download if you want to pick individual exports to download.")
else:
    export_report_names = [(export, export_name_to_report_mapping[export]) for export in export_names]
table_schema = dbutils.widgets.get("SCHEMA TO WRITE TO")

# COMMAND ----------

# MAGIC %md # Load Data Exports to DBX

# COMMAND ----------

from pycarlo.core import Client, Query, Mutation, Session
import requests
from pyspark.sql.functions import *
import io
import pandas as pd
from datetime import *

client = Client(session=Session(mcd_id=mcd_id, mcd_token=mcd_token,mcd_profile=mcd_profile))
today = datetime.today()

for export, report in export_report_names:
    print("Looking for Data Export: {}".format(export))
    query=Query()
    query.get_data_export_url(data_export_name=export).__fields__('url')
    report_url=client(query).get_data_export_url.url
    if not report_url:
        print("Data Export {} is not available right now.".format(export))
        print("\n")
        continue
    r = requests.get(report_url).content
    
    # Customize the naming scheme of the loaded tables here:
    table_name = "mcd_export_" + export.lower()
    filename = report
    
    #Read data into pandas to convert to csv
    df=pd.read_csv(io.StringIO(r.decode('utf-8')))  
    #display(df) #Uncomment to see the data before it is loaded to a table
    
    #changing column spaces to underscores (if there are any)
    df.columns = df.columns.str.replace(' ','_')
    print('Creating Spark Data Frame')
    DF = spark.createDataFrame(df).withColumn("load_date", lit(date(today.year, today.month, today.day)))

    #Load Data to Databricks DELTA lake
    DF.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{table_schema}.{table_name}")
    print("Created table: {}.{}".format(table_schema,table_name))
    print("\n") 

# COMMAND ----------

df = spark.sql("SHOW TABLES IN {} like 'mcd_export_*'".format(table_schema))
display(df)
