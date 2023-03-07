import json
import urllib3
import urllib.parse
import hmac
import hashlib
import os
from typing import Dict
import boto3  
from datetime import date


# Parameters - These are set in the Lambda Environment Variables
instance = os.environ.get('instance')
serviceNowUser = os.environ.get('serviceNowUser')
serviceNowPassword = os.environ.get('serviceNowPassword')
SHARED_SIGNING_SECRET = str.encode(os.environ.get('SHARED_SIGNING_SECRET'))  


def create_incident(url, table, incident_type, incident_id) :
    http = urllib3.PoolManager()
    endpoint = "https://"+instance +".service-now.com/api/now/table/incident"
    payload = json.dumps({
      "short_description": incident_type+ " " + table,
      "urgency": "3",
      "work_notes": "[code]<a href= "+url+" target='_blank'>Monte Carlo Incident</a[/code]>",
      "correlation_id" : "MC-"+incident_id 
    })

    authHeaders = urllib3.make_headers(basic_auth='{}:{}'.format(serviceNowUser, serviceNowPassword))

    basicHeaders = {
      'Content-Type': 'application/json',
      'Accept': 'application/json'}
    headers = {**authHeaders, **basicHeaders}  # Merge the two header dictionaries    
    
    response = http.request('POST', endpoint, headers=headers, body=payload)
  
    return( response.data.decode('utf-8'))

def lambda_handler(event, context):   
    
    mcd_signature = event['headers'].get('x-mcd-signature')
    body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']

    
    if verify_signature(mcd_signature=mcd_signature, body=body):
        print('Signature Verified!')
        
        #TODO null check vars below
        #Add description (Monitor Name) // Important for SQL RULE / Custom Monitors generally
        # Group ID is schema, would be good to add
        # Event details when available 
        # Look into number of tables effected (value?)
        # Language like 50 affected tables in this schema
        incident_url = body['payload'].get('url')
        incident_type = body['type']
        table_name = body['payload']["event_list"][0]["table_name"]
        incident_id = body['payload'].get('incident_id')

        r = create_incident(incident_url, table_name, incident_type, incident_id)
        return {'statusCode': 200}
    return {'statusCode': 403}
    
def verify_signature(mcd_signature: str, body: Dict) -> bool:
    body_as_byes = urllib.parse.urlencode(body).encode('utf8')
    computed_signature = hmac.new(SHARED_SIGNING_SECRET, body_as_byes, hashlib.sha512).hexdigest()
    print("Computed Signature: " + computed_signature)
    return hmac.compare_digest(computed_signature, mcd_signature)

## This is only a debugging method for logging requests to S3
def logToS3(log):
    encoded_string = log.encode("utf-8")
    bucket_name = "s3bucket"
    file_name = "request"+ date.today() + ".txt"
    s3_path = file_name
    s3 = boto3.resource("s3")
    s3.Bucket(bucket_name).put_object(Key=s3_path, Body=encoded_string)