import boto3
import json
import time
import logging
import argparse

# Define S3 and DynamoDB Clients
S3_CLIENT = boto3.client('s3')
DYNAMODB_CLIENT = boto3.client('dynamodb')
SQS_CLIENT = boto3.client('sqs')

logFile = 'consumer.log'
logging.basicConfig(
    filename=logFile,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def RetrieveRequest(source):
    try:
        # the request data's key is numeric with the same number of digits each time; thus, grabbing the first element should be the smallest key
        response = S3_CLIENT.list_objects_v2(Bucket=source, MaxKeys=1)
        if 'Contents' in response:
            key = response['Contents'][0]['Key']
            # obtain the actual content of the object
            myObject = S3_CLIENT.get_object(Bucket=source, Key=key)
            request = json.loads(myObject['Body'].read().decode('utf-8'))
            logging.info(f"Request with key {key} retrieved.")
            return request, key
    except Exception as e:
        logging.error(f"Error retrieving request: {e}")
    return None, None

def FetchFromQueue(queueURL, maxMessages=10, waitTime=20):
    response = SQS_CLIENT.receive_message(
        QueueUrl=queueURL,
        MaxNumberOfMessages=maxMessages,
        WaitTimeSeconds=waitTime
    )
    return response.get('Messages', [])

def DeleteFromQueue(queueURL, receiptHandle):
    SQS_CLIENT.delete_message(QueueUrl=queueURL, ReceiptHandle=receiptHandle)


def IsValidWidgetId(widgetId):
    if len(widgetId) != 36:
        return False
    # ensure the positions of the dashes are correct
    if widgetId[8] != "-" or widgetId[13] != "-" or widgetId[18] != "-" or widgetId[23] != "-":
        return False
    # check that all other characters are hexadecimal
    hexCharacters = set("0123456789abcdefABCDEF")
    for i, char in enumerate(widgetId):
        if i in [8, 13, 18, 23]:
            continue  # skip dash positions
        if char not in hexCharacters:
            return False
    return True

def ProcessRequest(request, destination, storage):
    widgetId = request["widgetId"]
    if IsValidWidgetId(widgetId):
        requestType = request["type"]
        if requestType == "create":
            CreateWidget(request, destination, storage)
        elif requestType == "delete":
            DeleteWidget(request, destination, storage)
        elif requestType == "update":
            UpdateWidget(request, destination, storage)
        else:
            logging.warning(f"Unknown request type: {requestType}")

def CreateWidget(request, destination, storage):
    try:
        widgetId = request["widgetId"]
        owner = request["owner"].replace(" ", "-").lower()
        data = json.dumps(request)
        # ensure 'other attributes' is at the top level of 'request' as per assignment description
        if "otherAttributes" in request:
            otherAttributes = request.pop("otherAttributes")
            request.update(otherAttributes)
        if storage == "s3":
            s3Key = f"widgets/{owner}/{widgetId}"
            data = json.dumps(request)
            S3_CLIENT.put_object(Body=data, Bucket=destination, Key=s3Key, ContentType='application/json')
            logging.info(f"Widget with ID {widgetId} stored in S3 at {s3Key}")
        elif storage == "dynamodb":   
            dynamoDict = {"id": {"S": widgetId}}
            for key, value in request.items():
                dynamoDict[key] = GetDynamoAttribute(value)
            DYNAMODB_CLIENT.put_item(TableName=destination, Item=dynamoDict)
            logging.info(f"Widget with ID {widgetId} stored in DynamoDB at {destination}")
    except Exception as e:
        logging.error(f"Error creating widget: {e}")

def UpdateWidget(request, destination, storage):
    widgetId = request["widgetId"]
    try:
        if storage == "s3":
            owner = request["owner"].replace(" ", "-").lower()
            s3Key = f"widgets/{owner}/{widgetId}"  # Assumes a certain directory structure for widgets
            data = json.dumps(request)
            S3_CLIENT.put_object(Body=data, Bucket=destination, Key=s3Key, ContentType='application/json')
            logging.info(f"Widget with ID {widgetId} updated in S3 at {s3Key}")
        elif storage == "dynamodb":
            dynamoDict = {"id": {"S": widgetId}}
            for key, value in request.items():
                dynamoDict[key] = GetDynamoAttribute(value)
            DYNAMODB_CLIENT.put_item(TableName=destination, Item=dynamoDict)
            logging.info(f"Widget with ID {widgetId} updated in DynamoDB at {destination}")
    except Exception as e:
        logging.error(f"Error updating widget with ID {widgetId}: {e}")

def DeleteWidget(widgetId, destination, storage):
    try:
        if storage == "s3":
            s3Key = f"widgets/{widgetId}"  # Assumes a certain directory structure for widgets
            S3_CLIENT.delete_object(Bucket=destination, Key=s3Key)
            logging.info(f"Widget with ID {widgetId} deleted from S3 at {s3Key}")
        elif storage == "dynamodb":
            DYNAMODB_CLIENT.delete_item(TableName=destination, Key={"id": {"S": widgetId}})
            logging.info(f"Widget with ID {widgetId} deleted from DynamoDB at {destination}")
    except Exception as e:
        logging.error(f"Error deleting widget with ID {widgetId}: {e}")

def GetDynamoAttribute(value):
    if isinstance(value, str):
        return {"S": value}
    elif isinstance(value, int) or isinstance(value, float):
        return {"N": str(value)}
    elif isinstance(value, list):
        return {"L": [GetDynamoAttribute(item) for item in value]}
    elif isinstance(value, dict):
        return {"M": {key: GetDynamoAttribute(val) for key, val in value.items()}}

def DeleteRequest(key, source):
    try:
        S3_CLIENT.delete_object(Bucket=source, Key=key)
        logging.info(f"Request with key {key} deleted from {source}")
    except Exception as e:
        logging.error(f"Error deleting request with key {key}: {e}")

def main(source, destination, storage, queueURL):
    while True:
        messages = FetchFromQueue(queueURL)
        for message in messages:
            body = json.loads(message['Body'])
            request, key = RetrieveRequest(source, body)  # Assumes body contains required info to retrieve S3 object.
            if request:
                ProcessRequest(request, destination, storage)
                DeleteRequest(key, source)
                DeleteFromQueue(queueURL, message['ReceiptHandle'])
        if not messages:
            time.sleep(.1)


# this will only run if consumer.py is run directly, not being imported
# i needed to place the command-line args here to let my unit test program work
if __name__ == "__main__":
    # Command-line arguments setup 
    # EXAMPLE: python3 consumer.py --request-source usu-cs5260-tylerj-requests --request-destination usu-cs5260-tylerj-web --storage-strategy s3
    # python3 consumer.py --request-source usu-cs5260-tylerj-requests --request-destination widgets --storage-strategy dynamodb
    parser = argparse.ArgumentParser(description='Consumer program to process requests to create, update, or delete widgets')
    parser.add_argument('--request-source', required=False, help='S3 bucket where the requests are fetched from')
    parser.add_argument('--request-destination', required=False, help='Choose where to store the widgets')
    parser.add_argument('--storage-strategy', required=False, choices=['s3', 'dynamodb'], help='Choose \'s3\' to store widgets in a bucket or \'dynamodb\' to store widgets in a dynamodb table')
    parser.add_argument('--queue-url', required=False, help='URL of the SQS queue')

    args = parser.parse_args()

    # Define command-line argument constants
    REQUEST_SOURCE = args.request_source
    REQUEST_DESTINATION = args.request_destination
    STORAGE_STRATEGY = args.storage_strategy
    QUEUE_URL = args.queue_url
    main(REQUEST_SOURCE, REQUEST_DESTINATION, STORAGE_STRATEGY, QUEUE_URL)