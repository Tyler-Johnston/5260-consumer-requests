import boto3
import json
import time
import logging
import argparse

# ---- SETUP CLIENTS / LOGGING ---- 
S3_CLIENT = boto3.client('s3')
DYNAMODB_CLIENT = boto3.client('dynamodb', "us-east-1")
SQS_CLIENT = boto3.client('sqs', "us-east-1")

# logFile = 'consumer.log'
logging.basicConfig(
    # filename=logFile,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ---- HELPER FUNCTIONS ----
def GetDynamoAttribute(value):
    if isinstance(value, str):
        return {"S": value}
    elif isinstance(value, int) or isinstance(value, float):
        return {"N": str(value)}
    elif isinstance(value, list):
        return {"L": [GetDynamoAttribute(item) for item in value]}
    elif isinstance(value, dict):
        return {"M": {key: GetDynamoAttribute(val) for key, val in value.items()}}

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

# ---- RETRIEVE REQUESTS FROM SOURCES ----
def RetrieveRequestFromS3(bucketSource):
    try:
        # the request data's key is numeric with the same number of digits each time; thus, grabbing the first element should be the smallest key
        response = S3_CLIENT.list_objects_v2(Bucket=bucketSource, MaxKeys=1)
        if 'Contents' in response:
            key = response['Contents'][0]['Key']
            # obtain the actual content of the object
            myObject = S3_CLIENT.get_object(Bucket=bucketSource, Key=key)
            request = json.loads(myObject['Body'].read().decode('utf-8'))
            logging.info(f"Request with key {key} retrieved.")
            return request, key
    except Exception as e:
        logging.error(f"Error retrieving request: {e}")
    return None, None

def RetrieveRequestsFromQueue(queueURL, maxMessages=10, waitTime=20):
    response = SQS_CLIENT.receive_message(
        QueueUrl=queueURL,
        MaxNumberOfMessages=maxMessages,
        WaitTimeSeconds=waitTime
    )
    return response.get('Messages', [])

# ---- PROCESS (CREATE/UPDATE/DELETE) REQUESTS AT THE DESTINATION -----
def ProcessRequest(request, destination, storageStrategy):
    widgetId = request["widgetId"]
    if IsValidWidgetId(widgetId):
        requestType = request["type"]
        if requestType == "create":
            CreateOrUpdateWidget(request, destination, storageStrategy, operation="created")
        elif requestType == "update":
            CreateOrUpdateWidget(request, destination, storageStrategy, operation="updated")
        elif requestType == "delete":
            DeleteWidget(widgetId, destination, storageStrategy)
        else:
            logging.warning(f"Unknown request type: {requestType}")

def CreateOrUpdateWidget(request, destination, storageStrategy, operation):
    try:
        widgetId = request["widgetId"]
        
        # ensure 'other attributes' is at the top level of 'request' as per assignment description
        if "otherAttributes" in request:
            otherAttributes = request.pop("otherAttributes")
            request.update(otherAttributes)

        if storageStrategy == "s3":
            owner = request["owner"].replace(" ", "-").lower()
            s3Key = f"widgets/{owner}/{widgetId}"
            data = json.dumps(request)
            S3_CLIENT.put_object(Body=data, Bucket=destination, Key=s3Key, ContentType='application/json')
            logging.info(f"Widget with ID {widgetId} {operation} in S3 at {destination}")

        elif storageStrategy == "dynamodb":   
            dynamoDict = {"id": {"S": widgetId}}
            for key, value in request.items():
                dynamoDict[key] = GetDynamoAttribute(value)
            DYNAMODB_CLIENT.put_item(TableName=destination, Item=dynamoDict)
            logging.info(f"Widget with ID {widgetId} {operation} in DynamoDB at {destination}")

    except Exception as e:
        logging.error(f"Error creating widget: {e}")

def DeleteWidget(widgetId, destination, storageStrategy):
    try:
        if storageStrategy == "s3":
            s3Key = f"widgets/{widgetId}"
            S3_CLIENT.delete_object(Bucket=destination, Key=s3Key)
            logging.info(f"Widget with ID {widgetId} deleted from S3 at {s3Key}")

        elif storageStrategy == "dynamodb":
            DYNAMODB_CLIENT.delete_item(TableName=destination, Key={"id": {"S": widgetId}})
            logging.info(f"Widget with ID {widgetId} deleted from DynamoDB at {destination}")

    except Exception as e:
        logging.error(f"Error deleting widget with ID {widgetId}: {e}")

# ---- DELETE FROM SOURCE AFTER PROCESSING ----
def DeleteFromStorage(key, bucketSource):
    try:
        S3_CLIENT.delete_object(Bucket=bucketSource, Key=key)
        logging.info(f"Request with key {key} deleted from {bucketSource}")
    except Exception as e:
        logging.error(f"Error deleting request with key {key}: {e}")

def DeleteFromQueue(queueURL, receiptHandle):
    SQS_CLIENT.delete_message(QueueUrl=queueURL, ReceiptHandle=receiptHandle)
    logging.info("Deleted a response from SQS")

# ---- DRIVER CODE ----
def main(bucketSource, destination, storageStrategy, queueURL):
    while True:
        if bucketSource and not queueURL:
            request, key = RetrieveRequestFromS3(bucketSource)
            if request:
                try:
                    ProcessRequest(request, destination, storageStrategy)
                    DeleteFromStorage(key, bucketSource)
                except Exception as e:
                    logging.error(f"Error processing request from storage: {e}")
            else:
                time.sleep(0.1)
        elif queueURL:
            requests = RetrieveRequestsFromQueue(queueURL)
            if requests:
                for request in requests:
                    try:
                        messageBody = json.loads(request['Body'])
                        ProcessRequest(messageBody, destination, storageStrategy)
                        DeleteFromQueue(queueURL, request['ReceiptHandle'])
                    except Exception as e:
                        logging.error(f"Error processing request from queue: {e}")
            else:
                time.sleep(0.1)
        else:
            logging.error("Neither storage nor queue URL was specified. Exiting...")
            break

# ---- COMMAND-LINE ARGS SETUP ----
if __name__ == "__main__":
    # EXAMPLES:
    # python3 consumer.py --request-source usu-cs5260-tylerj-requests --request-destination usu-cs5260-tylerj-web --storage-strategy dynamodb
    # python3 consumer.py --queue-url https://sqs.us-east-1.amazonaws.com/487854293488/cs5260-requests --request-destination usu-cs5260-tylerj-web --storage-strategy s3

    parser = argparse.ArgumentParser(description='Consumer program to process requests to create, update, or delete widgets')
    parser.add_argument('--storage-strategy', required=True, choices=['s3', 'dynamodb'], help='Choose \'s3\' to store widgets in a bucket or \'dynamodb\' to store widgets in a dynamodb table')
    parser.add_argument('--request-destination', required=True, help='Choose where to store the widgets')

    # initialize a mutually exclusive group
    group = parser.add_mutually_exclusive_group(required=True)

    # adding request-source and queue-url ensures one of them is required but not both
    group.add_argument('--request-source', help='S3 bucket where the requests are fetched from')
    group.add_argument('--queue-url', help='URL of the SQS queue')

    args = parser.parse_args()

    REQUEST_SOURCE = args.request_source # optional - the s3 bucket it is coming from
    REQUEST_DESTINATION = args.request_destination # required
    STORAGE_STRATEGY = args.storage_strategy # required
    QUEUE_URL = args.queue_url # optional - the sqs queue it is coming from

    main(REQUEST_SOURCE, REQUEST_DESTINATION, STORAGE_STRATEGY, QUEUE_URL)