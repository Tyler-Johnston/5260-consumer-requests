import boto3
import json
import time
import logging
import argparse

# Define S3 and DynamoDB Clients
S3_CLIENT = boto3.client('s3')
DYNAMODB_CLIENT = boto3.client('dynamodb', )

logging.basicConfig(level=logging.INFO)

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

def ProcessRequest(request, destination, storage):
    requestType = request["type"]
    if requestType == "create":
        CreateWidget(request, destination, storage)
    elif requestType == "delete":
        #TODO: add this delete functionality later
        logging.info("Delete request found. Skipping for now...")
    elif requestType == "update":
        #TODO: add this update functionality later
        logging.info("Update request found. Skipping for now...")
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
            dynamoDict = {"Item": {}}
            for key, value in request.items():
                dynamoDict["Item"][key] = GetDynamoAttribute(value)
            DYNAMODB_CLIENT.put_item(TableName=destination, Item=dynamoDict)
            logging.info(f"Widget with ID {widgetId} stored in DynamoDB at {destination}")
    except Exception as e:
        logging.error(f"Error creating widget: {e}")

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

def main(source, destination, storage):
    while True:
        request, key = RetrieveRequest(source)
        if request:
            ProcessRequest(request, destination, storage)
            DeleteRequest(key, source)
        else:
            time.sleep(.1)

if __name__ == "__main__":
    # Command-line arguments setup 
    # EXAMPLE: python consumer.py --request-source usu-cs5260-tylerj-requests --request-destination usu-cs5260-tylerj-web --storage-strategy s3
    parser = argparse.ArgumentParser(description='Consumer program to process requests to create, update, or delete widgets')
    parser.add_argument('--request-source', required=True, help='S3 bucket where the requests are fetched from')
    parser.add_argument('--request-destination', required=True, help='Choose where to store the widgets')
    parser.add_argument('--storage-strategy', required=True, choices=['s3', 'dynamodb'], help='Choose \'s3\' to store widgets in a bucket or \'dynamodb\' to store widgets in a dynamodb table')
    args = parser.parse_args()

    # Define command-line argument constants
    REQUEST_SOURCE = args.request_source
    REQUEST_DESTINATION = args.request_destination
    STORAGE_STRATEGY = args.storage_strategy
    main(REQUEST_SOURCE, REQUEST_DESTINATION, STORAGE_STRATEGY)