import json
import boto3

def lambda_handler(event, context):
    try:
        request_data = event
    except json.JSONDecodeError:
        return {
            'statusCode': 400,
            'body': json.dumps({'message': 'Invalid JSON'})
        }

    # validate the request data
    if not validate_request(request_data):
        return {
            'statusCode': 400,
            'body': json.dumps({'message': 'Invalid request data'})
        }

    # send the response to my SQS
    queue_url = 'https://sqs.us-east-1.amazonaws.com/487854293488/cs5260-requests'
    sqs_response = send_to_sqs(request_data, queue_url)
    return sqs_response

def validate_request(data):
    required_fields = ['type', 'requestId', 'widgetId', 'owner', 'label', 'description', 'otherAttributes']
    for field in required_fields:
        if field not in data:
            return False
    return True

def send_to_sqs(data, queue_url):
    sqs = boto3.client('sqs')
    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(data)
    )
    return response
