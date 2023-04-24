# Databricks notebook source
# MAGIC %md # Environment Setup

# COMMAND ----------

#Only run this if you have not installed pycarlo on the DBX cluster already
#pip install pycarlo

# COMMAND ----------

# DBTITLE 1,Runtime Variables
# Preferred: Monte Carlo Credentials stored in DBX Secret Key Repo called "monte-carlo-creds":
#mcd_id = dbutils.secrets.get(scope = "monte-carlo-creds", key = "mc-key-id")
#mcd_token = dbutils.secrets.get(scope = "monte-carlo-creds", key = "mc-secret-id")

#Otherwise hardcode for testing purposes only!
mcd_id = 'your-id-here'
mcd_token = 'your-secret-here'

# Full List of Reports Needed, Edit as Needed for Databricks
insight_names = ["monitors","cleanup_suggestions","events","incident_history"]
insight_report_names = ["monitors.csv","cleanup_suggestions.csv","events.csv","incident_history.csv"]

# Other variables which you should customize as needed:
mcd_profile=""
table_schema = 'com_us_alyt_04'
user = 'gordon.strodel@takeda.com'

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
    table_name = "mc_src_"+insight_names[i]
    filename = insight_report_names[i]
    
    df=pd.read_csv(io.StringIO(r.decode('utf-8')))  
    #display(df) #Uncomment to see the data before it is loaded to a table
    
    #changing column spaces to underscores (if there are any)
    df.columns = df.columns.str.replace(' ','_')
    print('Creating Spark Data Frame')
    DF = spark.createDataFrame(df).withColumn("load_date", lit(date(today.year, today.month, today.day)))
    dbutils.fs.rm("dbfs:/FileStore/shared_uploads/{}/{}".format(table_name,user), True)  
    DF.write.format("delta").mode("overwrite").option("mergeSchema", "true").save("dbfs:/FileStore/shared_uploads/{}/{}".format(user,table_name))  
    spark.sql("DROP TABLE if exists {}.{}".format(table_schema,table_name))  
    spark.sql("CREATE TABLE {}.{} USING DELTA LOCATION 'dbfs:/FileStore/shared_uploads/{}/{}'".format(table_schema,table_name,user,table_name))  
    print("Created table: {}.{}".format(table_schema,table_name))
    print("\n") 

# COMMAND ----------

df=spark.sql("SHOW TABLES IN {} like 'mc*'".format(table_schema))  
display(df)
