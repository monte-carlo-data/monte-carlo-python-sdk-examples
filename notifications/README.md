# Instructions for Implementing Webhooks Notifications

## Google Chat

todo

## ServiceNow

[ServiceNow Integration](https://docs.getmontecarlo.com/docs/servicenow)

## Webex Teams

### Overview

`webex_lambda.py` code for a Lambda function that can receive webhooks from Monte Carlo and post messages to a Webex Teams room.

For more information about Webex webhooks, see the [Webex Teams Webhooks Guide](https://developer.webex.com/docs/api/guides/webhooks).

For more information about webhooks from Monte Carlo, see the [Monte Carlo Webhooks Guide](https://docs.getmontecarlo.com/docs/webhooks).

### Webex Prerequisites

1. Create a Webex Bot and obtain an access token. For more information, see the [Webex Teams Bot Guide](https://developer.webex.com/docs/bots).
2. Create a Webex Teams room where you want to post messages. For more information, see the [Webex Teams Rooms Guide](https://developer.webex.com/docs/api/v1/rooms).
3. Invite the Webex Bot to the room where you want to post messages.
4. Get the room ID for the room where you want to post messages.This is available in the Webex Teams room URL. For example, if the room link is `webexteams://im?space=5ee4bc50-6a0a-11ee-8c80-19de48a0d1a2`, the room ID is `5ee4bc50-6a0a-11ee-8c80-19de48a0d1a2`.

### Lamda Setup

1. Navigate to the [AWS Lambda console](https://console.aws.amazon.com/lambda/home).
2. Click **Create function**.
3. Select **Author from scratch**.
4. Enter a name for the function.
5. Select **Python 3.11** for the runtime.
6. Select **Create a new role with basic Lambda permissions** for the role.
7. Click **Create function**.
8. In the **Function code** panel, paste the contents of `webex_lambda.py`.
9. In the **Environment variables** panel, add the following variables:
    * `WEBEX_ACCESS_TOKEN`: The access token for the Webex Teams bot.
    * `WEBEX_ROOM_ID`: The room ID for the Webex Teams room where you want to post messages.
    * `WEBEX_ENDPOINT`: The Webex Teams API endpoint. This is always `https://webexapis.com/v1/messages`.
    * `SHARED_SIGNING_SECRET`: The shared secret that you configured in the Monte Carlo webhook configuration.
10. Click **Add trigger**.
11. Select **API Gateway**.
12. Select **Create a new API**.
13. For Security, select **Open**. This is only for the initial setup. You can configure security later.
14. Click **Add**.
15. Record the API Gateway endpoint URL. You will need this for the Monte Carlo webhook configuration.
16. Click **Deploy API**.

### Monte Carlo Setup

1. Navigate to the [Monte Carlo webhooks configuration page](https://getmontecarlo.com/settings/notifications).
2. Select **Add notification**.
3. Select **Custom** or **Automated Monitors**
4. For `Channel` select **Webhook**
5. For `Recipient` enter the URL for the API Gateway endpoint that you created in the Lambda setup. For example, `https://1234567890.execute-api.us-east-1.amazonaws.com/default/webex_lambda`.
6. Select other options as desired.
7. Click **Test Notification** to test the webhook.
8. Click **Add notification** to save the webhook.
