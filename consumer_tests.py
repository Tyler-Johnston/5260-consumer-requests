import unittest
from unittest.mock import patch, Mock
import json
import os
from consumer import RetrieveRequestFromS3, RetrieveRequestsFromQueue, ProcessRequest, CreateOrUpdateWidget, DeleteWidget, DeleteFromStorage, DeleteFromQueue, IsValidWidgetId
from lambda_function import *

# place the folder of sample requests into a list for testing purposes
def LoadSampleRequests(directory):
    requests = []
    for filename in os.listdir(directory):
        filePath = os.path.join(directory, filename)
        with open(filePath, 'r') as file:
            try:
                data = json.load(file)
                if IsValidWidgetId(data['widgetId']):
                    requests.append(data)
            except json.JSONDecodeError:
                print(f"Error decoding JSON in file: {filePath}")
    return requests

SAMPLE_REQUESTS = LoadSampleRequests("sample-requests/")

class TestRetrieveRequestFromS3(unittest.TestCase):

    @patch('consumer.S3_CLIENT.list_objects_v2')
    @patch('consumer.S3_CLIENT.get_object')
    def test_retrieve_request_from_s3(self, mock_get_object, mock_list_objects):
        mock_list_objects.return_value = {'Contents': [{'Key': '1234'}]}
        for myRequest in SAMPLE_REQUESTS:
            with self.subTest(myRequest=myRequest):
                mock_get_object.return_value = {'Body': Mock(read=lambda: json.dumps(myRequest).encode('utf-8'))}
                request, key = RetrieveRequestFromS3("some-bucket")
                self.assertEqual(key, '1234')
                self.assertEqual(request, myRequest)

    @patch('consumer.SQS_CLIENT.receive_message')
    def test_retrieve_requests_from_queue(self, mock_receive_message):
        # prepare the mock SQS messages
        mock_sqs_messages = [{'Body': json.dumps(request)} for request in SAMPLE_REQUESTS]

        # mock response from SQS as it would be received
        mock_response = {'Messages': mock_sqs_messages}
        mock_receive_message.return_value = mock_response

        # the queue URL for the test (can be a fake one for testing purposes)
        queue_url = 'https://sqs.fake-region.amazonaws.com/123456789012/my-queue'

        # when RetrieveRequestsFromQueue is called, it should now receive our mock messages
        messages = RetrieveRequestsFromQueue(queue_url)

        # assertions to check if the correct number of messages were retrieved and they match the sample requests
        self.assertEqual(len(messages), len(SAMPLE_REQUESTS))
        for i, message in enumerate(messages):
            self.assertEqual(json.loads(message['Body']), SAMPLE_REQUESTS[i])

        # validate that the mock was called with the expected parameters
        mock_receive_message.assert_called_once_with(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20
        )
        
class TestProcessRequest(unittest.TestCase):

    @patch('consumer.CreateOrUpdateWidget')
    @patch('consumer.DeleteWidget')
    def test_process_functions_called_once(self, mock_delete_widget, mock_create_update_widget):
        for request in SAMPLE_REQUESTS:
            with self.subTest(request=request):
                ProcessRequest(request, 'destination', 's3')  # Call ProcessRequest once per iteration
                
                # check the number of times CreateOrUpdateWidget was called
                if request['type'] == 'create' or request['type'] == 'update':
                    mock_create_update_widget.assert_called_once()
                else:
                    mock_create_update_widget.assert_not_called()

                # check the number of times DeleteWidget was called
                if request['type'] == 'delete':
                    mock_delete_widget.assert_called_once()
                else:
                    mock_delete_widget.assert_not_called()

                # reset mocks at the end of each iteration
                mock_create_update_widget.reset_mock()
                mock_delete_widget.reset_mock()
class TestCreateOrUpdateWidget(unittest.TestCase):

    @patch('consumer.S3_CLIENT.put_object')
    def test_create_widget_s3(self, mock_put_object):
        for request in SAMPLE_REQUESTS:
            with self.subTest(request=request):
                CreateOrUpdateWidget(request, 'destination', 's3', operation="created")
                # make sure the call to add items to s3 is only called once
                mock_put_object.assert_called_once()

                # make that the S3 key matches the expected format
                callArgs = mock_put_object.call_args[1]
                expectedS3Key = f"widgets/{request['owner'].replace(' ', '-').lower()}/{request['widgetId']}"
                self.assertEqual(callArgs['Key'], expectedS3Key)

                # make sure bucket destination is the one passed in
                self.assertEqual(callArgs["Bucket"], 'destination')

                # make sure the data being passed into s3 is the proper json format
                data = callArgs["Body"]
                try:
                    data = json.loads(data)
                except Exception as e:
                    self.fail("body data is not valid JSON")

                mock_put_object.reset_mock()

    @patch('consumer.DYNAMODB_CLIENT.put_item')
    def test_create_widget_dynamodb(self, mock_put_item):
        for request in SAMPLE_REQUESTS:
            with self.subTest(request=request):
                CreateOrUpdateWidget(request, 'destination', 'dynamodb', operation="created")
                # make sure the call to add items to dynamo db is only called once
                mock_put_item.assert_called_once()

                # make sure 'dynamoDict' contains the required structure elements
                callArgs = mock_put_item.call_args[1]  # obtain the keyword arguments
                self.assertTrue((callArgs['TableName'], "destination"))
                self.assertTrue(isinstance(callArgs['Item'], dict))
                itemDict = callArgs['Item']
                self.assertTrue('id' in itemDict)
                self.assertTrue(isinstance(itemDict['id'], dict))
                self.assertTrue('type' in itemDict)
                self.assertTrue(isinstance(itemDict['type'], dict))
                self.assertTrue('requestId' in itemDict)
                self.assertTrue(isinstance(itemDict['requestId'], dict))
                self.assertTrue('widgetId' in itemDict)
                self.assertTrue(isinstance(itemDict['widgetId'], dict))

                mock_put_item.reset_mock()

class TestDeleteRequest(unittest.TestCase):

    @patch('consumer.S3_CLIENT.delete_object')
    def test_delete_from_s3(self, mock_delete_object):
        DeleteFromStorage('1234', 'some-bucket')
        mock_delete_object.assert_called_once_with(Bucket='some-bucket', Key='1234')

    @patch('consumer.SQS_CLIENT.delete_message')
    def test_delete_from_queue(self, mock_delete_message):
        receipt_handle = 'SomeReceiptHandle'
        queue_url = 'https://sqs.some-region.amazonaws.com/123456789012/my-queue'
        DeleteFromQueue(queue_url, receipt_handle)
        mock_delete_message.assert_called_once_with(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )


class TestValidation(unittest.TestCase):

    def setUp(self):
        self.valid_data = {
            "type": "create",
            "requestId": "e80fab52-71a5-4a76-8c4d-11b66b83ca2a",
            "widgetId": "8123f304-f23f-440b-a6d3-80e979fa4cd6",
            "owner": "Mary Matthews",
            "label": "JWJYY",
            "description": "THBRNVNQPYAWNHGRGUKIOWCKXIVNDLWOIQTADHVEVMUAJWDONEPUEAXDITDSHJTDLCMHHSESFXSDZJCBLGIKKPUYAWKQAQI",
            "otherAttributes": [
                {
                "name": "width-unit",
                "value": "cm"
                },
                {
                "name": "length-unit",
                "value": "cm"
                },
                {
                "name": "rating",
                "value": "2.580677"
                },
                {
                "name": "note",
                "value": "FEGYXHIJCTYNUMNMGZBEIDLKXYFNHFLVDYZRNWUDQAKQSVFLPRJTTXARVEIFDOLTUSWZZWVERNWPPOEYSUFAKKAPAGUALGXNDOVPNKQQKYWWOUHGOJWKAJGUXXBXLWAKJCIVPJYRMRWMHRUVBGVILZRMESQQJRBLXISNFCXGGUFZCLYAVLRFMJFLTBOTLKQRLWXALLBINWALJEMUVPNJWWRWLTRIBIDEARTCSLZEDLZRCJGSMKUOZQUWDGLIVILTCXLFIJIULXIFGRCANQPITKQYAKTPBUJAMGYLSXMLVIOROSBSXTTRULFYPDFJSFOMCUGDOZCKEUIUMKMMIRKUEOMVLYJNJQSMVNRTNGH"
                }
            ]
        }

    def test_validate_request_valid(self):
        # Test with valid data
        self.assertTrue(validate_request(self.valid_data))

    def test_validate_request_invalid_type(self):
        # Test with invalid 'type' field type
        data = self.valid_data.copy()
        data['type'] = 123  # Non-string type
        self.assertFalse(validate_request(data))

    def test_validate_request_invalid_type_value(self):
        # Test with invalid 'type' field value
        data = self.valid_data.copy()
        data['type'] = 'badtype'  # Invalid type value
        self.assertFalse(validate_request(data))

    def test_validate_request_valid_type_create(self):
        data = self.valid_data.copy()
        data['type'] = 'create'
        self.assertTrue(validate_request(data))

    def test_validate_request_valid_type_update(self):
        data = self.valid_data.copy()
        data['type'] = 'update'
        self.assertTrue(validate_request(data))

    def test_validate_request_valid_type_delete(self):
        data = self.valid_data.copy()
        data['type'] = 'delete'
        self.assertTrue(validate_request(data))

    def test_validate_request_missing_field(self):
        # Test with missing 'owner' field
        data = self.valid_data.copy()
        del data['owner']
        self.assertFalse(validate_request(data))

    def test_validate_request_invalid_otherAttributes_structure(self):
        # Test with invalid structure in 'otherAttributes'
        data = self.valid_data.copy()
        data['otherAttributes'][0]['name'] = ''
        self.assertFalse(validate_request(data))


class TestLambdaHandler(unittest.TestCase):

    @patch('lambda_function.send_to_sqs')
    def test_successful_sqs_send(self, mock_send_to_sqs):
        mock_send_to_sqs.return_value = {'statusCode': 200, 'body': json.dumps({'message': 'Success'})}

        valid_event = {
            "type": "create",
            "requestId": "e80fab52-71a5-4a76-8c4d-11b66b83ca2a",
            "widgetId": "8123f304-f23f-440b-a6d3-80e979fa4cd6",
            "owner": "Mary Matthews",
            "label": "JWJYY",
            "description": "THBRNVNQPYAWNHGRGUKIOWCKXIVNDLWOIQTADHVEVMUAJWDONEPUEAXDITDSHJTDLCMHHSESFXSDZJCBLGIKKPUYAWKQAQI",
            "otherAttributes": [
                {
                "name": "width-unit",
                "value": "cm"
                },
                {
                "name": "length-unit",
                "value": "cm"
                },
                {
                "name": "rating",
                "value": "2.580677"
                },
                {
                "name": "note",
                "value": "FEGYXHIJCTYNUMNMGZBEIDLKXYFNHFLVDYZRNWUDQAKQSVFLPRJTTXARVEIFDOLTUSWZZWVERNWPPOEYSUFAKKAPAGUALGXNDOVPNKQQKYWWOUHGOJWKAJGUXXBXLWAKJCIVPJYRMRWMHRUVBGVILZRMESQQJRBLXISNFCXGGUFZCLYAVLRFMJFLTBOTLKQRLWXALLBINWALJEMUVPNJWWRWLTRIBIDEARTCSLZEDLZRCJGSMKUOZQUWDGLIVILTCXLFIJIULXIFGRCANQPITKQYAKTPBUJAMGYLSXMLVIOROSBSXTTRULFYPDFJSFOMCUGDOZCKEUIUMKMMIRKUEOMVLYJNJQSMVNRTNGH"
                }
            ]
        }

        response = lambda_handler(valid_event, {})
        self.assertEqual(response['statusCode'], 200)
        mock_send_to_sqs.assert_called_once_with(valid_event, 'https://sqs.us-east-1.amazonaws.com/487854293488/cs5260-requests')

    @patch('lambda_function.send_to_sqs')
    def test_sqs_send_failure(self, mock_send_to_sqs):
        mock_send_to_sqs.side_effect = Exception("SQS error")

        valid_event = {
            "type": "create",
            "requestId": "e80fab52-71a5-4a76-8c4d-11b66b83ca2a",
            "widgetId": "8123f304-f23f-440b-a6d3-80e979fa4cd6",
            "owner": "Mary Matthews",
            "label": "JWJYY",
            "description": "THBRNVNQPYAWNHGRGUKIOWCKXIVNDLWOIQTADHVEVMUAJWDONEPUEAXDITDSHJTDLCMHHSESFXSDZJCBLGIKKPUYAWKQAQI",
            "otherAttributes": [
                {
                "name": "width-unit",
                "value": "cm"
                },
                {
                "name": "length-unit",
                "value": "cm"
                },
                {
                "name": "rating",
                "value": "2.580677"
                },
                {
                "name": "note",
                "value": "FEGYXHIJCTYNUMNMGZBEIDLKXYFNHFLVDYZRNWUDQAKQSVFLPRJTTXARVEIFDOLTUSWZZWVERNWPPOEYSUFAKKAPAGUALGXNDOVPNKQQKYWWOUHGOJWKAJGUXXBXLWAKJCIVPJYRMRWMHRUVBGVILZRMESQQJRBLXISNFCXGGUFZCLYAVLRFMJFLTBOTLKQRLWXALLBINWALJEMUVPNJWWRWLTRIBIDEARTCSLZEDLZRCJGSMKUOZQUWDGLIVILTCXLFIJIULXIFGRCANQPITKQYAKTPBUJAMGYLSXMLVIOROSBSXTTRULFYPDFJSFOMCUGDOZCKEUIUMKMMIRKUEOMVLYJNJQSMVNRTNGH"
                }
            ]
        }

        response = lambda_handler(valid_event, {})
        self.assertEqual(response['statusCode'], 500)
        self.assertIn("SQS error", response['body'])



if __name__ == '__main__':
    unittest.main()