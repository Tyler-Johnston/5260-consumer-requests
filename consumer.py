import boto3
import json
import time
import logging
import argparse

# Command-line arguments setup
parser = argparse.ArgumentParser(description='Consumer program to process requests to create, update, or delete widgets')
parser.add_argument('--request-source', required=True, help='S3 bucket where the requests are fetched from.')
parser.add_argument('--request-destination', required=True, help='Choose where to store the widgets.')
args = parser.parse_args()

REQUESTS_SOURCE = args.request_source
REQUESTS_DESTINATION = args.request_destination

logging.basicConfig(level=logging.INFO)
client = boto3.client('s3')

def RetrieveRequest():
    # Logic to retrieve a single Widget Request from Bucket 2.
    pass

def ProcessRequest(request):
    requestType = request["type"]
    if requestType == "create":
        CreateWidget(request)
    #TODO: add delete / update functionality in the next iteration of this assignemnt
    elif requestType == "delete":
        logging.info("Delete request found. Skipping for now...")
    elif requestType == "update":
        logging.info("Update request found. Skipping for now...")
    else:
        logging.warning(f"Unknown request type: {requestType}")

def DeleteRequest(request):
    pass

def main():
    while True:
        request = RetrieveRequest()
        if request:
            ProcessRequest(request)
            DeleteRequest(request)
        else:
            time.sleep(0.1)

if __name__ == "__main__":
    main()
