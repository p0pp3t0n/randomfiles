import unittest
from unittest.mock import patch, MagicMock
from el import es, tabulate, sys

class TestElasticsearchScript(unittest.TestCase):

    @patch('el.Elasticsearch')
    def test_successful_connection_and_data_retrieval(self, mock_es):
        # Mock Elasticsearch client
        mock_client = MagicMock()
        mock_client.ping.return_value = True
        mock_client.search.return_value = {
            "hits": {
                "hits": [
                    {
                        "_id": "1",
                        "_source": {
                            "attributes": {
                                "title": "Test Title",
                                "timeFieldName": "timestamp",
                                "fields": ["field1", "field2"]
                            }
                        }
                    }
                ]
            }
        }
        mock_es.return_value = mock_client

        # Run the script
        with patch('builtins.print') as mock_print:
            import el
            el.es = mock_client

        # Assertions
        mock_print.assert_any_call(tabulate(
            [{"ID": "1", "Title": "Test Title", "Time Field": "timestamp", "Field Count": 2}],
            headers="keys",
            tablefmt="grid"
        ))

    @patch('el.Elasticsearch')
    def test_connection_failure(self, mock_es):
        # Mock Elasticsearch client
        mock_client = MagicMock()
        mock_client.ping.return_value = False
        mock_es.return_value = mock_client

        # Run the script
        with patch('builtins.print') as mock_print:
            import el
            el.es = mock_client

        # Assertions
        mock_print.assert_any_call("Connection error: Failed to connect to Elasticsearch cluster.")
        self.assertEqual(sys.exit.call_args[0][0], 1)

    @patch('el.Elasticsearch')
    def test_authentication_failure(self, mock_es):
        # Mock Elasticsearch client to raise AuthenticationException
        mock_client = MagicMock()
        mock_client.ping.side_effect = el.AuthenticationException("Invalid credentials")
        mock_es.return_value = mock_client

        # Run the script
        with patch('builtins.print') as mock_print:
            import el
            el.es = mock_client

        # Assertions
        mock_print.assert_any_call("Authentication failed: Invalid credentials")
        self.assertEqual(sys.exit.call_args[0][0], 1)

    @patch('el.Elasticsearch')
    def test_index_not_found(self, mock_es):
        # Mock Elasticsearch client to raise NotFoundError
        mock_client = MagicMock()
        mock_client.ping.return_value = True
        mock_client.search.side_effect = el.NotFoundError("Index not found")
        mock_es.return_value = mock_client

        # Run the script
        with patch('builtins.print') as mock_print:
            import el
            el.es = mock_client

        # Assertions
        mock_print.assert_any_call("Index '.kibana' not found.")
        self.assertEqual(sys.exit.call_args[0][0], 1)

    @patch('el.Elasticsearch')
    def test_missing_keys_in_document(self, mock_es):
        # Mock Elasticsearch client
        mock_client = MagicMock()
        mock_client.ping.return_value = True
        mock_client.search.return_value = {
            "hits": {
                "hits": [
                    {
                        "_id": "1",
                        "_source": {
                            "attributes": {
                                "title": "Test Title"
                                # Missing 'timeFieldName' and 'fields'
                            }
                        }
                    }
                ]
            }
        }
        mock_es.return_value = mock_client

        # Run the script
        with patch('builtins.print') as mock_print:
            import el
            el.es = mock_client

        # Assertions
        mock_print.assert_any_call("Missing expected key in document: 'timeFieldName'")

if __name__ == '__main__':
    unittest.main()
