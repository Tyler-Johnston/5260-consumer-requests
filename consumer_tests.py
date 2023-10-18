import unittest
from unittest.mock import patch, Mock
from consumer import RetrieveRequest, ProcessRequest, CreateWidget, DeleteRequest

SAMPLE_REQUEST = {
    "type": "create",
    "requestId": "e80fab52-71a5-4a76-8c4d-11b66b83ca2a",
    "widgetId": "8123f304-f23f-440b-a6d3-80e979fa4cd6",
    "owner": "Mary Matthews",
    "label": "JWJYY",
    "description": "THBRNVNQPYAWNHGRGUKIOWCKXIVNDLWOIQTADHVEVMUAJWDONEPUEAXDITDSHJTDLCMHHSESFXSDZJCBLGIKKPUYAWKQAQI",
    "otherAttributes": [
        {"name": "width-unit", "value": "cm"},
        {"name": "length-unit", "value": "cm"},
        {"name": "rating", "value": "2.580677"},
        {"name": "note", "value": "FEGYXHIJCTYNUMNMGZBEIDLKXYFNHFLVDYZRNWUDQAKQSVFLPRJTTXARVEIFDOLTUSWZZWVERNWPPOEYSUFAKKAPAGUALGXNDOVPNKQQKYWWOUHGOJWKAJGUXXBXLWAKJCIVPJYRMRWMHRUVBGVILZRMESQQJRBLXISNFCXGGUFZCLYAVLRFMJFLTBOTLKQRLWXALLBINWALJEMUVPNJWWRWLTRIBIDEARTCSLZEDLZRCJGSMKUOZQUWDGLIVILTCXLFIJIULXIFGRCANQPITKQYAKTPBUJAMGYLSXMLVIOROSBSXTTRULFYPDFJSFOMCUGDOZCKEUIUMKMMIRKUEOMVLYJNJQSMVNRTNGH"}
    ]
}

class TestRetrieveRequest(unittest.TestCase):

    @patch('consumer.S3_CLIENT.list_objects_v2')
    @patch('consumer.S3_CLIENT.get_object')
    def test_retrieve_request(self, mock_get_object, mock_list_objects):
        mock_list_objects.return_value = {
            'Contents': [{'Key': '1234'}]
        }
        mock_get_object.return_value = {
            'Body': Mock(read=lambda: json.dumps(SAMPLE_REQUEST).encode('utf-8'))
        }
        request, key = RetrieveRequest()
        self.assertEqual(key, '1234')
        self.assertEqual(request, SAMPLE_REQUEST)

class TestProcessRequest(unittest.TestCase):

    @patch('consumer.CreateWidget')
    def test_process_create_request(self, mock_create_widget):
        ProcessRequest(SAMPLE_REQUEST)
        mock_create_widget.assert_called_once()

    @patch('consumer.CreateWidget')
    def test_process_delete_request(self, mock_create_widget):
        delete_request = SAMPLE_REQUEST.copy()
        delete_request['type'] = 'delete'
        ProcessRequest(delete_request)
        mock_create_widget.assert_not_called()

class TestCreateWidget(unittest.TestCase):

    @patch('consumer.S3_CLIENT.put_object')
    def test_create_widget_s3(self, mock_put_object):
        with patch('consumer.STORAGE_STRATEGY', 's3'):
            CreateWidget(SAMPLE_REQUEST)
            mock_put_object.assert_called_once()

    @patch('consumer.DYNAMODB_CLIENT.put_item')
    def test_create_widget_dynamodb(self, mock_put_item):
        with patch('consumer.STORAGE_STRATEGY', 'dynamodb'):
            CreateWidget(SAMPLE_REQUEST)
            mock_put_item.assert_called_once()

class TestDeleteRequest(unittest.TestCase):

    @patch('consumer.S3_CLIENT.delete_object')
    def test_delete_request(self, mock_delete_object):
        DeleteRequest('1234')
        mock_delete_object.assert_called_once_with(Bucket='YOUR_REQUEST_SOURCE', Key='1234')

if __name__ == '__main__':
    unittest.main()
