import boto3
import json
import time
import logging
import argparse

#TODO: double check this is how the command-line args are passed
parser = argparse.ArgumentParser(description='Process S3 buckets / DyanmoDB table for requests and widgets.')
parser.add_argument('--request-bucket', required=True, help='S3 bucket where the requests are fetched from.')
parser.add_argument('--widget-bucket', required=True, help='S3 bucket / DynamoDB table where the processed requests need to go.')
args = parser.parse_args()

REQUESTS_SOURCE = args.request_bucket
REQUESTS_DESTINATION = args.widget_bucket

logging.basicConfig(level=logging.INFO)
s3_client = boto3.client('s3')

def RetrieveRequest():
    pass

def ProcessRequest():
    pass

def DeleteRequest():
    pass

def CreateRequest():
    pass

def main():
    # loop until some stop condition is met
        # try to get the request (RetrieveRequest)
        # if got request:
            # process the request  (ProcessRequest)
        # else:
            # wait a while (100 ms)
    pass
    

if __name__ == "__main__":
    main()

