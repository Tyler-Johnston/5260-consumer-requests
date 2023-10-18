import unittest
from unittest.mock import patch, Mock
import json
import os
from consumer import RetrieveRequest, ProcessRequest, CreateWidget, DeleteRequest, IsValidWidgetId

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
class TestRetrieveRequest(unittest.TestCase):

    @patch('consumer.S3_CLIENT.list_objects_v2')
    @patch('consumer.S3_CLIENT.get_object')
    def test_retrieve_request(self, mock_get_object, mock_list_objects):
        mock_list_objects.return_value = {
            'Contents': [{'Key': '1234'}]
        }
        for myRequest in SAMPLE_REQUESTS:
            with self.subTest(myRequest=myRequest):
                mock_get_object.return_value = {
                    'Body': Mock(read=lambda: json.dumps(myRequest).encode('utf-8'))
                }
                request, key = RetrieveRequest('some-bucket')
                self.assertEqual(key, '1234')
                self.assertEqual(request, myRequest)

class TestProcessRequest(unittest.TestCase):

    @patch('consumer.CreateWidget')
    def test_process_create_request(self, mock_create_widget):
        for request in SAMPLE_REQUESTS:
            with self.subTest(request=request):
                if request['type'] == 'create':
                    ProcessRequest(request, 'destination', 's3')
                    mock_create_widget.assert_called_once()
                elif request['type'] != 'create':
                    ProcessRequest(request, 'destination', 's3')
                    mock_create_widget.assert_not_called()
                mock_create_widget.reset_mock()

class TestCreateWidget(unittest.TestCase):

    @patch('consumer.S3_CLIENT.put_object')
    def test_create_widget_s3(self, mock_put_object):
        for request in SAMPLE_REQUESTS:
            with self.subTest(request=request):
                CreateWidget(request, 'destination', 's3')
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
                CreateWidget(request, 'destination', 'dynamodb')
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
    def test_delete_request(self, mock_delete_object):
        DeleteRequest('1234', 'some-bucket')
        mock_delete_object.assert_called_once_with(Bucket='some-bucket', Key='1234')

if __name__ == '__main__':
    unittest.main()