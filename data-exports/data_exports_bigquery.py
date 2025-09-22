#Usage Instructions:
#1. Create a Service Account within BigQuery with Owner-level permissions
#2. Create a Key (JSON) from this Service Account, save locally and specify the path below under "key_path"
#3. Input your Bigquery Project ID under "bq_project_id"
#4. Configure the Monte Carlo CLI with a --profile-name to reference in variable mcd_profile (https://docs.getmontecarlo.com/docs/using-the-cli#setting-up-the-cli)
#5. Update the data_export_names and data_export_report_names for the specific reports you want to include (associated names should be in same index)
#NOTES: 
# - This will create local CSV files for all data imported to BigQuery
# - Running this script will overwrite existing dataset and tables with same names in BQ project

from pycarlo.core import Client, Query, Mutation, Session
import csv
import requests
# from google.cloud import bigquery
# from google.oauth2 import service_account

#-------------------INPUT VARIABLES---------------------
key_path = ""
bq_project_id=""
mcd_profile=""

#testing
mcd_id=""
mcd_token=""

data_export_names = ['MONITORS','ALERTS','EVENTS','ASSETS']
data_export_report_names = ['monitors.csv',"alerts.csv","events.csv","assets.csv"]
#-------------------------------------------------------

credentials = service_account.Credentials.from_service_account_file(key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"])
bq_client = bigquery.Client(credentials=credentials, project=bq_project_id, location="US")
dataset_id = "monte_carlo_data_exports"
client = Client(session=Session(mcd_profile=mcd_profile))

#testing
# client = Client(session=Session(mcd_id=mcd_id,mcd_token=mcd_token))

bq_client.create_dataset("monte_carlo_data_exports", exists_ok = True)
for report in data_export_names:
	bq_client.create_table(bq_project_id+".monte_carlo_data_exports."+report, exists_ok = True)
	print("Created {} Table in {} dataset".format(report,dataset_id))

for i in range(len(data_export_report_names)):
	print(data_export_names[i])
	query=Query()
	query.get_data_export_url(data_export_name=data_export_names[i]).__fields__('url')
	report_url=client(query).get_data_export_url.url
	r = requests.get(report_url)
	url_content = r.content
	with open(data_export_report_names[i],"wb") as report_csv:
		report_csv.write(url_content)
		report_csv.close()

	table_id = data_export_names[i]
	filename = data_export_report_names[i]

	BIGQUERY LOAD
	dataset_ref = bq_client.dataset(dataset_id)
	table_ref = dataset_ref.table(table_id)
	job_config = bigquery.LoadJobConfig()
	job_config.write_disposition = 'WRITE_TRUNCATE'
	job_config.allow_quoted_newlines = True
	job_config.source_format = bigquery.SourceFormat.CSV
	job_config.autodetect = True

	with open(filename,"rb") as source_file:
		job = bq_client.load_table_from_file(source_file, table_ref, job_config=job_config)
		job.result()
		print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))
		source_file.close()
