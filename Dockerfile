FROM python:3.9-slim
COPY consumer.py ./
RUN pip install boto3
# normally wouldn't hard-code parameters, but this is a simple proof-of-concept of using docker
CMD ["python3", "consumer.py", "--queue-url", "https://sqs.us-east-1.amazonaws.com/487854293488/cs5260-requests", "--request-destination", "usu-cs5260-tylerj-web", "--storage-strategy", "s3"]