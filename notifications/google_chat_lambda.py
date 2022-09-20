"""
Example MCD Webhook to Google Chat Lambda
"""

import hashlib
import hmac
import json
import urllib.parse
import urllib3
from typing import Dict
import os

SHARED_SIGNING_SECRET = str.encode(os.environ.get('SHARED_SIGNING_SECRET'))  # This should be an environment variable

def lambda_handler(event: Dict, context: Dict) -> Dict:
    mcd_signature = event['headers'].get('x-mcd-signature')
    body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
    
    if verify_signature(mcd_signature=mcd_signature, body=body):
        print('Signature Verified!')
        google_webhook(body)
        return {'statusCode': 200}
    return {'statusCode': 403}


def verify_signature(mcd_signature: str, body: Dict) -> bool:
    # return True
    body_as_byes = urllib.parse.urlencode(body).encode('utf8')
    computed_signature = hmac.new(SHARED_SIGNING_SECRET, body_as_byes, hashlib.sha512).hexdigest()
    return hmac.compare_digest(computed_signature, mcd_signature)
    
def google_webhook(body):
    """Hangouts Chat incoming webhook quickstart."""
    url = os.environ.get('google_endpoint')
    type = body['type']
    http = urllib3.PoolManager()
    message_text = '''
    New Incident
    Type: {}
    URL: {}
    '''.format(type,url)
    message_headers = {'Content-Type': 'application/json; charset=UTF-8'}
    bot_message = {
        'text': message_text
    }
    encoded_message = json.dumps(bot_message)
    response = http.request("POST",url,headers = message_headers, body = encoded_message)
    print(response.data.decode('utf-8'))