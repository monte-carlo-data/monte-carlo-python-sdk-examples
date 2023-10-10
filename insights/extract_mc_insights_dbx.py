# Databricks notebook source
# MAGIC %md # Instructions
# MAGIC ## What is this?
# MAGIC This notebook will download insights from Monte Carlo using the Monte Carlo API and then load them into Delta Table(s). Each insight will be loaded to its own Delta Table. This script will create / replace the Delta Table each time it is run. The table names will be "mcd_insight_insightname"
# MAGIC
# MAGIC ## Prerequisites
# MAGIC * Through the Monte Carlo UI create an API token.
# MAGIC * Store the Token ID and Token Value in a DBX Secret key repo named 'monte-carlo-creds' with the keys 'mcd-id' and 'mcd-token'
# MAGIC     * Alternatively you can set the ID and Token in this notebook direclty by editing the cell of this notebook named 'Find/Set API Credentials'
# MAGIC * This script will not create a _schema_ for you. It is assumed that the schema you provide already exists.
# MAGIC
# MAGIC ## Running the notebook
# MAGIC * After the 'Create User Input Widgets' command is run, there will be two drop down widgets at the top of the notebook
# MAGIC   * INSIGHTS TO DOWNLOAD: Lets you select which insight(s) you want to downlaod. The default will be ALL. If you want to only download a set of specific insights, de-select ALL and select the insights you want.
# MAGIC   * SCHEMA TO WRITE TO: The schema under which the Delta Tables will be created/replaced.
# MAGIC * Run the rest of the commands to download the insights from Monte Carlo and import them to Databricks

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

# DBTITLE 1,Build a List of Available Reports
from pycarlo.core import Client, Query, Session

client = Client(session=Session(mcd_id=mcd_id, mcd_token=mcd_token, mcd_profile=mcd_profile))
query = Query()
query.get_insights().__fields__('name', 'reports')

response = client(query).get_insights

insight_name_to_report_mapping = {}
for insight in response:
    name = insight.name

    for report in insight.reports:
        # Some Insights have a .html report as well, we want to filter for just the .csv reports
        if report.name.endswith('.csv'):
            insight_name_to_report_mapping[name] = report.name

# COMMAND ----------

# DBTITLE 1,Create User Input Widgets
dbutils.widgets.multiselect(
    'INSIGHTS TO DOWNLOAD',
    defaultValue='ALL',
    choices=['ALL'] + list(insight_name_to_report_mapping.keys())
)
dbutils.widgets.text("SCHEMA TO WRITE TO", "mcd_insights")

# COMMAND ----------

# DBTITLE 1,Runtime Variables (Pulled From Input Widgets)
insight_names = dbutils.widgets.get("INSIGHTS TO DOWNLOAD").split(',')

# If ALL is in list of insight_names selected, even if other individual insights are selected, we will download all insights
if insight_names == ['ALL']:
    insight_report_names = [insight_name_to_report_mapping[insight] for insight in
                            list(insight_name_to_report_mapping.keys())]
elif 'ALL' in insight_names:
    raise Exception("De-select 'ALL' from Insights to Download if you want to pick individual insights to download.")
else:
    insight_report_names = [insight_name_to_report_mapping[insight] for insight in insight_names]
table_schema = dbutils.widgets.get("SCHEMA TO WRITE TO")

# COMMAND ----------

# MAGIC %md # Load Insights to DBX

# COMMAND ----------

from pycarlo.core import Client, Query, Mutation, Session
import requests
from pyspark.sql.functions import *
import io
import pandas as pd
from datetime import *

client = Client(session=Session(mcd_id=mcd_id, mcd_token=mcd_token,mcd_profile=mcd_profile))
today = datetime.today()

for i in range(len(insight_report_names)):
    print("Looking for Insight Report: {}".format(insight_report_names[i]))
    query=Query()
    query.get_report_url(insight_name=insight_names[i],report_name=insight_report_names[i]).__fields__('url')
    report_url=client(query).get_report_url.url
    r = requests.get(report_url).content
    
    # Customize the naming scheme of the loaded tables here:
    table_name = "mcd_insight_" + insight_names[i]
    filename = insight_report_names[i]
    
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

df = spark.sql("SHOW TABLES IN {} like 'mcd_insight_*'".format(table_schema))
display(df)
