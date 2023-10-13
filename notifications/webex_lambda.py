import hashlib
import hmac
import json
import urllib.parse
import urllib3
from typing import Dict
import os

SHARED_SIGNING_SECRET = os.environ.get('SHARED_SIGNING_SECRET') # any secret used to validate MC incoming webhooks
WEBEX_ENDPOINT = os.environ.get('WEBEX_ENDPOINT') # always 'https://webexapis.com/v1/messages'
WEBEX_ROOM_ID = os.environ.get('WEBEX_ROOM_ID') # room id of the channel where MC will deliver messages. get this from webex
WEBEX_TOKEN = os.environ.get('WEBEX_TOKEN') # create a new bot here to retrieve token: https://developer.webex.com/my-apps/new

def lambda_handler(event: Dict, context: Dict) -> Dict:
    mcd_signature = event['headers'].get('x-mcd-signature')
    body = json.loads(event['body'])
    
    if verify_signature(mcd_signature=mcd_signature, body=body):
        print('Success!')
        webex_webhook(body)
        return {'statusCode': 200}
    return {'statusCode': 403}  
    
def verify_signature(mcd_signature: str, body: Dict) -> bool:
    body_as_bytes = urllib.parse.urlencode(body).encode('utf8')
    computed_signature = hmac.new(SHARED_SIGNING_SECRET.encode(), body_as_bytes, hashlib.sha512).hexdigest()
    return hmac.compare_digest(computed_signature, mcd_signature)

def webex_webhook(body):
    """Hangouts Chat incoming webhook quickstart."""
    url = os.environ.get('WEBEX_ENDPOINT')
    type = body['type']
    http = urllib3.PoolManager()
    message_text = '''
    New Incident
    Type: {}
    URL: {}
    '''.format(type,body['payload']['url'])
    message_headers = {'Authorization': f'Bearer {WEBEX_TOKEN}',
    'Content-Type': 'application/json'}
    bot_message = {
        'text': message_text,
        'roomId': f'{WEBEX_ROOM_ID}'
    }
    encoded_message = json.dumps(bot_message)
    response = http.request("POST",url,headers = message_headers, body = encoded_message)
    print(response.data.decode('utf-8'))
