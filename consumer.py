import boto3
import json
import time
import logging
import argparse

# Command-line arguments setup
parser = argparse.ArgumentParser(description='Consumer program to process requests to create, update, or delete widgets')
parser.add_argument('--request-source', required=True, help='S3 bucket where the requests are fetched from')
parser.add_argument('--request-destination', required=True, help='Choose where to store the widgets')
parser.add_argument('--storage-strategy', required=True, choices=['s3', 'dynamodb'], help='Choose \'s3\' to store widgets in a bucket or \'dynamodb\' to store widgets in a dynamodb table')
args = parser.parse_args()

# Define command-line argument constants
REQUEST_SOURCE = args.request_source
REQUEST_DESTINATION = args.request_destination
STORAGE_STRATEGY = args.storage_strategy

# Define S3 and DynamoDB Clients
S3_CLIENT = boto3.client('s3')
DYNAMODB_CLIENT = boto3.client('dynamodb')

logging.basicConfig(level=logging.INFO)

def RetrieveRequest():
    try:
        # the request data's key is numeric with the same number of digits each time; thus, grabbing the first element should be the smallest key
        response = S3_CLIENT.list_objects_v2(Bucket=REQUEST_SOURCE, MaxKeys=1)
        if 'Contents' in response:
            firstObjectKey = response['Contents'][0]['Key']
            # obtin the actual content of the object
            myObject = S3_CLIENT.get_object(Bucket=REQUEST_SOURCE, Key=firstObjectKey)
            data = json.loads(myObject['Body'].read().decode('utf-8'))
            return data
    except Exception as e:
        logging.error(f"Error retrieving request: {e}")
    return None

def ProcessRequest(request):
    requestType = request["type"]
    if requestType == "create":
        CreateWidget(request)
    elif requestType == "delete":
        DeleteWidget(request)
    elif requestType == "update":
        #TODO: add this update functionality later
        logging.info("Update request found. Skipping for now...")
    else:
        logging.warning(f"Unknown request type: {requestType}")

def CreateWidget(request):
    try:
        data = json.dumps(request)
        widgetId = request["widgetId"]
        if STORAGE_STRATEGY == "s3":
            S3_CLIENT.put_object(Body=data, Bucket=REQUEST_DESTINATION, Key=widgetId, ContentType='application/json')
            logging.info(f"Widget with ID {widgetId} stored in S3 at {REQUEST_DESTINATION}")
        elif STORAGE_STRATEGY == "dynamodb":   
            #TODO: handle creating dynamodb side of things
            # dynamodb.put_item(TableName=REQUEST_DESTINATION, Item= pass)
            logging.info(f"Widget with ID {widgetId} stored in DynamoDB at {REQUEST_DESTINATION}")
    except Exception as e:
        logging.error(f"Error creating widget: {e}")

def DeleteWidget(key):
    try:
        S3_CLIENT.delete_object(Bucket=REQUEST_SOURCE, Key=key)
        logging.info(f"Request with key {key} deleted from {REQUEST_SOURCE}")
    except Exception as e:
        logging.error(f"Error deleting request with key {key}: {e}")

def main():
    while True:
        request = RetrieveRequest()
        if request:
            ProcessRequest(request)
            DeleteRequest(request)
        else:
            time.sleep(.5)

if __name__ == "__main__":
    main()