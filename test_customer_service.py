import unittest
from unittest.mock import patch, Mock
import customer_service

class TestCustomerService(unittest.TestCase):

    @patch('customer_service.requests.get')
    def test_fetch_customers_success(self, mock_get):
        mock_response = Mock()
        mock_response.json.return_value = [{'firstname': 'Alice', 'lastname': 'Smith'}]
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        customers = customer_service.fetch_customers()
        self.assertIsNotNone(customers)
        self.assertEqual(customers[0]['firstname'], 'Alice')

    @patch('customer_service.requests.get')
    def test_fetch_customers_empty_list(self, mock_get):
        mock_response = Mock()
        mock_response.json.return_value = []
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        customers = customer_service.fetch_customers()
        self.assertEqual(customers, [])

    @patch('customer_service.requests.get')
    def test_fetch_customers_http_error(self, mock_get):
        mock_get.side_effect = requests.exceptions.RequestException("Server down")
        customers = customer_service.fetch_customers()
        self.assertIsNone(customers)

    def test_log_first_customer_with_firstname(self):
        with self.assertLogs('customer_fetcher', level='INFO') as cm:
            customer_service.log_first_customer([{'firstname': 'Bob', 'lastname': 'Brown'}])
            self.assertIn("First customer's firstname: Bob", "\n".join(cm.output))

    def test_log_first_customer_missing_firstname(self):
        with self.assertLogs('customer_fetcher', level='WARNING') as cm:
            customer_service.log_first_customer([{'lastname': 'Brown'}])
            self.assertIn("Firstname not found in the first customer object.", "\n".join(cm.output))

    def test_log_first_customer_empty_list(self):
        with self.assertLogs('customer_fetcher', level='WARNING') as cm:
            customer_service.log_first_customer([])
            self.assertIn("Customer list is empty.", "\n".join(cm.output))

if __name__ == '__main__':
    unittest.main()
