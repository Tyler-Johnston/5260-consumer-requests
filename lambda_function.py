import json
import boto3
import re
import logging

# logFile = 'lambda_function.log'
logging.basicConfig(
    # filename=logFile,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def lambda_handler(event, context):
    try:
        request_data = event

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
        
    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': f'Error occurred: {str(e)}'})
        }

def validate_request(data):
    # Check required fields
    required_fields = ['type', 'requestId', 'widgetId', 'owner', 'label', 'description', 'otherAttributes']
    for field in required_fields:
        if field not in data:
            return False

    # Check if fields are non-empty strings
    for field in ['type', 'requestId', 'widgetId', 'owner', 'label', 'description']:
        if not isinstance(data[field], str) or not data[field].strip():
            return False

    # Check for valid types
    valid_types = ['create', 'update', 'delete']
    if data['type'].lower() not in valid_types:
        return False

    # Check UUID format for requestId and widgetId
    uuid_regex = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\Z', re.I)
    if not re.match(uuid_regex, data['requestId']) or not re.match(uuid_regex, data['widgetId']):
        return False

    # Validate otherAttributes structure
    if not isinstance(data['otherAttributes'], list):
        return False
    for attr in data['otherAttributes']:
        if not (isinstance(attr, dict) and 'name' in attr and 'value' in attr):
            return False
        if not (isinstance(attr['name'], str) and attr['name'].strip() and 
                isinstance(attr['value'], str) and attr['value'].strip()):
            return False

    return True

def send_to_sqs(data, queue_url):
    sqs = boto3.client('sqs')
    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(data)
    )
    return response
