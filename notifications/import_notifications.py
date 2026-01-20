# INSTRUCTIONS:
# NOTE: Only works if Monte Carlo environment has a single resource connected
# 1. Pass your api keys when running script, and pass name of .yml file you want to write to
# 2. .YML file will be written, which can then be used when syncing Notifications-as-code
# 3. Older notifications created from UI may not have the `audiences` object in YAML file - Need to check with MC team on this

import os
import yaml
import requests
import textwrap
from pycarlo.core import Client, Query, Session

MONTECARLO_API_URL = 'https://api.getmontecarlo.com/graphql'

# For more details about API call, refer Monte Carlo official documentation - https://docs.getmontecarlo.com/docs/using-the-api
def get_data_from_montecarlo(url, mcdId, mcdToken):
    '''
    Function to fetch notifications from code via API call
    '''
    headers = {'Content-type': 'application/json', 'x-mcd-id': mcdId, 'x-mcd-token': mcdToken}
    # Fetch below `query` value by inspecting notifications api call from browser
    json_data = {
        'query': """query getNotificationsSettings {\n
                            getUser {\n
                                account {\n
                                    notificationSettings {\n
                                        type\n
                                        recipients\n
                                        notificationScheduleType\n
                                        anomalyTypes\n
                                        incidentSubTypes\n
                                        uuid\n
                                        specificationRule\n
                                        recipientsDisplayNames\n
                                        customMessage\n
                                        notificationEnabled\n
                                        isTemplateManaged\n
                                        notificationCountHistory {\n
                                            day\n
                                            notificationCount\n
                                            __typename\n
                                        }\n
                                        digestSettings {\n
                                            digestType\n
                                            createdTime\n
                                            id\n
                                            intervalMinutes\n
                                            nextExecutionTime\n
                                            startTime\n
                                            uuid\n
                                            __typename\n
                                        }\n
                                        routingRules {\n
                                            tableStatsRules\n
                                            tableRules\n
                                            tableIdRules\n
                                            tagRules\n
                                            sqlRules\n
                                            domainRules\n
                                            monitorLabels\n
                                            monitorLabelsMatchType\n
                                            id\n
                                            uuid\n
                                            __typename\n
                                        }\n
                                        extra\n
                                        createdBy {\n
                                            email\n
                                            __typename\n
                                        }\n
                                        __typename\n
                                    }\n
                                    slackCredentials {\n
                                        id\n
                                        __typename\n
                                    }\n
                                    slackCredentials {\n
                                        id\n
                                        __typename\n
                                    }\n
                                    slackCredentials {\n
                                        id\n
                                        __typename\n
                                    }\n
                                    __typename\n
                                }\n
                                __typename\n
                            }\n
                        }""",
        'variables': {},
    }
    response = requests.post(url=url, headers=headers, json=json_data)
    # Extract relevant information by parsing json response
    response_dict = response.json()['data']['getUser']['account']['notificationSettings']
    notifications_uuid_list = [x['uuid'] for x in response_dict]
    return notifications_uuid_list

def export_yaml_template(notificationUuids):
    query = Query()
    get_yaml = query.export_monte_carlo_config_templates(notification_uuids=notificationUuids, export_name=True)
    get_yaml.__fields__("config_template_as_yaml")
    return query

def add_audiences_attribute_if_missing(notifications_yaml_object):
    notifications_json = yaml.safe_load(
                        notifications_yaml_object["config_template_as_yaml"]
                    )
    for dict_attribute in notifications_json['notifications']['slack']:
        if ('audiences' not in dict_attribute.keys()):
            dict_attribute['audiences'] = dict_attribute['channel']
    notifications_yaml_final = yaml.dump(notifications_json, allow_unicode=True, sort_keys=False)
    return notifications_yaml_final


def bulk_export_yaml(mcdId, mcdToken, fileName):
    client = Client(session=Session(mcd_id=mcdId, mcd_token=mcdToken))
    notifications_uuid_list = get_data_from_montecarlo(url=MONTECARLO_API_URL, mcdId=mcdId, mcdToken=mcdToken)
    with open(fileName, "w") as yaml_file:
        yaml_file.write("montecarlo:\n")
        # Export API response into montecarlo yaml template
        notifications_yaml = client(
                    export_yaml_template(notifications_uuid_list)
                ).export_monte_carlo_config_templates

        notifications_yaml_final = add_audiences_attribute_if_missing(notifications_yaml)
        yaml_file.write(
                    textwrap.indent(
                        notifications_yaml_final, prefix="  "
                    )
                )
    print("Notifications YAML generated!")

if __name__ == "__main__":
    # -------------------INPUT VARIABLES---------------------
    mcdId = os.getenv("MCD_DEFAULT_API_ID")
    mcdToken = os.getenv("MCD_DEFAULT_API_TOKEN")
    fileName = "notifications.yml"
    # -------------------------------------------------------
    bulk_export_yaml(mcdId, mcdToken, fileName)
