0. How to connect to EC2 instance:
- chmod 400 /path/to/your-key.pem
- ssh -i /path/to/your-key.pem ec2-user@your-ec2-public-dns

1. <b>producer.jar</b>:
- the producer.jar is responsible for generating widget-related requests. These requests are data structures that instruct how a widget should be created, updated, or deleted.
- Once a request is generated, producer.jar stores this request in an S3 bucket as an object. Each object in S3 has a unique key, which helps identify and retrieve it.
- After storing the request in S3, producer.jar sends a message to an Amazon Simple Queue Service (SQS) queue. This message doesn't contain the full request data. Instead, it contains information (like the S3 object's key) that points to the request stored in the S3 bucket. Essentially, the message in SQS acts as a pointer or reference to the actual request data in S3.

2. <b>consumer.py</b>:
- This script runs in a loop, constantly checking the SQS queue for new messages. 
- When it finds a message in the queue, it uses the information in that message to retrieve the corresponding request from the S3 bucket. The RetrieveRequest function handles this by fetching the object (request data) from the S3 bucket using the provided key.
- Once the request is retrieved, the consumer.py script processes it. Depending on the request's instructions, it can:
    - Create a new widget
    - Update an existing widget
    - Delete a widget
- The actual widget data, based on the request's content, can be stored in a different S3 bucket or a DynamoDB table, depending on the specified storage strategy.
- After the request has been processed, the script deletes the message from the SQS queue using the DeleteFromQueue function. It also deletes the request from the S3 bucket using the DeleteRequest function to ensure it doesn't process the same request again.


## how to use producer.jar to populate SQS:
ava -jar producer.jar -rq https://sqs.us-east-1.amazonaws.com/...

<!-- aws s3 cp s3://usu-cs5260-tylerj-requests/instructor-producer.jar 

java -jar consumer.jar --request-bucket=usu-cs5260-tylerj-requests --widget-bucket=usu-cs5260-tylerj-web

java -jar producer.jar --request-bucket=usu-cs5260-tylerj-requests
java -jar producer.jar --sqs-queue-name=cs5260-requests

aws s3 cp s3://usu-cs5260-tylerj-dist/instructor-consumer.jar ./ -->


---

Define a New Function to Send to SQS:

Before processing the widget request, we'll send the request to the SQS queue. Here's a simple function to achieve that:

python

def SendToSQS(queueURL, messageBody):
    try:
        SQS_CLIENT.send_message(QueueUrl=queueURL, MessageBody=json.dumps(messageBody))
        logging.info(f"Request sent to SQS: {messageBody}")
    except Exception as e:
        logging.error(f"Error sending message to SQS: {e}")

Modify the RetrieveRequest Function:

After retrieving the request from the S3 bucket, send it to the SQS queue:

python

def RetrieveRequest(source, queueURL):
    try:
        # ... (rest of the code)

        # After successfully retrieving the request, send it to SQS
        SendToSQS(queueURL, request)

        logging.info(f"Request with key {key} retrieved.")
        return request, key
    except Exception as e:
        logging.error(f"Error retrieving request: {e}")
    return None, None

Modify the main Function:

Change the while loop to first fetch from the queue and then process the request:

python

def main(source, destination, storage, queueURL):
    while True:
        # Instead of fetching from the S3 bucket, fetch from the SQS queue:
        messages = FetchFromQueue(queueURL)
        for message in messages:
            request = json.loads(message['Body'])
            if request:
                ProcessRequest(request, destination, storage)
                # No need to delete from S3 here as you're processing from SQS
                DeleteFromQueue(queueURL, message['ReceiptHandle'])
        if not messages:
            time.sleep(.1)

The line request, key = RetrieveRequest(source, body) is removed from the main loop because now you're directly fetching from the SQS queue, and the request is the message body.

Ensure Command-Line Argument for SQS URL is Required:

Make the queue-url command-line argument mandatory:

python

parser.add_argument('--queue-url', required=True, help='URL of the SQS queue')

Run the Consumer:

You'd run the consumer specifying the SQS queue URL:

bash

    python3 consumer.py --request-source usu-cs5260-tylerj-requests --request-destination usu-cs5260-tylerj-web --storage-strategy s3 --queue-url YOUR_SQS_QUEUE_URL

By making these changes, the Consumer will add the widget requests to the SQS queue after they're retrieved from the S3 bucket. Then, the main loop will process these requests directly from the SQS queue, allowing for efficient and scalable processing.
