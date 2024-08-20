from pycarlo.core import Client, Session
from datetime import datetime
import json


class Log:
    def __init__(self, client, output_path) -> None:
        self.client = client
        self.output_path = output_path
        self.responses = {}
        self.set_start_variables()
    
    def set_start_variables(self):
        self.has_next_page = True
        self.end_cursor = ''
        self.variables = ''
        self.query_executions = 0

    def runQuery(self, query, api_signature):
        if self.query_executions == 0:
            self.responses[api_signature] = []
        response = self.client(query)
        self.responses[api_signature].extend(response[api_signature]['records'])
        self.has_next_page = response[api_signature]['page_info']['has_next_page']
        self.end_cursor = response[api_signature]['page_info']['end_cursor']
        self.query_executions += 1
        

    def getAccountAuditLogs(self, start_time):
        while self.has_next_page:
            
            if self.query_executions == 0:
                self.variables = 'startTime: "{0}"'.format(datetime.isoformat(start_time))
            else:
                self.variables = 'startTime: "{0}" after: "{1}"'.format(datetime.isoformat(start_time), self.end_cursor)
            
            query = '''
                    query GetAccountAuditLogs {
                    getAccountAuditLogs(''' + self.variables + ''') {
                        pageInfo {
                        endCursor
                        hasNextPage
                        hasPreviousPage
                        startCursor
                        }
                        records {
                        accountName
                        accountUuid
                        apiCallReferences
                        apiCallSource
                        apiIsQuery
                        apiName
                        clientIp
                        email
                        eventType
                        firstName
                        lastName
                        timestamp
                        url
                        }
                    }
                    }'''
            
            self.runQuery(query, 'get_account_audit_logs')
        
        self.set_start_variables()

    def write_logs(self, start_time):
        file_name = 'audit_logs_'  + start_time.strftime('%Y_%m_%d_%H%M%S') + '.json'
        with open(self.output_path + file_name, 'w') as outfile:
            json.dump(self.responses, outfile)


if __name__ == '__main__':
    #-------------------INPUT VARIABLES---------------------
    mcdId = '{Insert key here}'
    mcdToken = '{Insert token here}'
    output_path = '{Insert path here}'
    # Select date to export
    start_time = datetime(2024,8,6,0,0,0)
	#-------------------------------------------------------
    print("Creating session and getting audit log")
    client=Client(session=Session(mcd_id=mcdId,mcd_token=mcdToken))
    
    
    
    audit_log = Log(client, output_path)
    audit_log.getAccountAuditLogs(start_time)

    audit_log.write_logs(start_time)
    print("Writing logs complete")
