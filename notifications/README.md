# Instructions for Implementing Webhooks Notifications

The scripts in this folder contain examples on how to run webhooks in AWS with the use of Lambda and an API Gateway.

## Steps

1. Log into your AWS environment, navigating to the Lambda section.
2. Create a new lambda, pasting the lambda found here and deploying.
3. Create an API Gateway for Monte Carlo to communicate directly with the lambda.
4. Create a Function URL, used later in step 10.
5. Within the 3rd party notification app, follow any instructions to create a webhook endpoint.
6. Copy this webhook url for step 8.
7. Within AWS while viewing the lambda, navigate to Configuration -> Environment Variables.
8. Create any variables that are referenced in the lambda function. See each service section for more details.
9. Create a notification within Monte Carlo, selecting Webhook as the channel.
10. Once you have completed the routing logic, pass the Function URL from step 4 as the Webhook URL, and the SHARED_SIGNING_SECRET from step 8 as the Secret.

## Services
### Google Chat

#### Overview

`google_chat_lambda.py` code for a Lambda function that can receive webhooks from Monte Carlo and post messages to a Google Chat.

#### Environment Variables

In the **Environment variables** panel, add the following variables:
* `GOOGLE_ENDPOINT`: The Google Chat API endpoint.
* `SHARED_SIGNING_SECRET`: The shared secret that you configured in the Monte Carlo webhook configuration.

### Webex Teams

#### Overview

`webex_lambda.py` code for a Lambda function that can receive webhooks from Monte Carlo and post messages to a Webex Teams room.

For more information about Webex webhooks, see the [Webex Teams Webhooks Guide](https://developer.webex.com/docs/api/guides/webhooks).

For more information about webhooks from Monte Carlo, see the [Monte Carlo Webhooks Guide](https://docs.getmontecarlo.com/docs/webhooks).

#### Webex Prerequisites

1. Create a Webex Bot and obtain an access token. For more information, see the [Webex Teams Bot Guide](https://developer.webex.com/docs/bots).
2. Create a Webex Teams room where you want to post messages. For more information, see the [Webex Teams Rooms Guide](https://developer.webex.com/docs/api/v1/rooms).
3. Invite the Webex Bot to the room where you want to post messages.
4. Get the room ID for the room where you want to post messages.This is available in the Webex Teams room URL. For example, if the room link is `webexteams://im?space=5ee4bc50-6a0a-11ee-8c80-19de48a0d1a2`, the room ID is `5ee4bc50-6a0a-11ee-8c80-19de48a0d1a2`.

#### Environment Variables

In the **Environment variables** panel, add the following variables:
* `WEBEX_ACCESS_TOKEN`: The access token for the Webex Teams bot.
* `WEBEX_ROOM_ID`: The room ID for the Webex Teams room where you want to post messages.
* `WEBEX_ENDPOINT`: The Webex Teams API endpoint. This is always `https://webexapis.com/v1/messages`.
* `SHARED_SIGNING_SECRET`: The shared secret that you configured in the Monte Carlo webhook configuration.

### ServiceNow

`service_now_lambda.py` **[DEPRECATED]** 

Refer to the built-in 
[ServiceNow Integration](https://docs.getmontecarlo.com/docs/servicenow)