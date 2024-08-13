# Notification Webhooks
The scripts in this folder contain examples on how to run webhooks in AWS with the use of Lambda and an API Gateway.
## Steps
1. Log into your AWS environment, navigating to the Lambda section.
2. Create a new lambda, pasting the lambda found here and deploying.
3. Create an API Gateway for Monte Carlo to communicate directly with the lambda.
4. Create a Function URL, used later in step 11.
5. Within the 3rd party notification app, follow any instructions to create a webhook endpoint.
6. Copy this webhook url for step 7.
7. Within AWS while viewing the lambda, navigate to Configuration -> Environment Variables.
8. Create any variables that are referenced in the lambda function i.e. google_endpoint, providing the webhook url from steps 5 and 6 as the value.
9. Create another variable entitled SHARED_SIGNING_SECRET, in which you can create your own secure secret value (this will be used later in step 11).
10. Create a notification within Monte Carlo, selecting Webhook as the channel.
11. Once you have completed the routing logic, pass the Function URL from step 4 as the Webhook URL, and the SHARED_SIGNING_SECRET from step 9 as the Secret.
