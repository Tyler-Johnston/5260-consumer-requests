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
java -jar producer.jar -rq https://sqs.us-east-1.amazonaws.com/...

java -jar producer.jar --request-bucket=usu-cs5250-green-requests
java -jar producer.jar -rb=usu-cs5250-green-requests -mwr 10 -ird 500

## how to send files from local computer to ec2 instance:
scp -i /path/to/your-key.pem /path/to/local/file.txt ec2-user@your-ec2-ip-address:/home/ec2-user/


## API

https://api-id.execute-api.us-east-1.amazonaws.com/widget/

