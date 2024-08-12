import sys
import os
import unittest
from azure.functions import HttpRequest, HttpResponse

# Add the src and api_ingestion_function directories to the system path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src', 'api_ingestion_function')))

from api_ingestion_function.function_app import main

class TestFunctionApp(unittest.TestCase):

    def setUp(self):
        """Set up test variables and mock environment variables"""
        self.method = "GET"
        self.url = "http://localhost/api_ingestion"
        self.headers = {}
        self.params = {}
        self.body = None

         # Ensure the environment variables are set; no defaults provided in the code
        os.environ["KEY_VAULT_URI"] = os.getenv("KEY_VAULT_URI")
        os.environ["BLOB_STORAGE_URL"] = os.getenv("BLOB_STORAGE_URL")
        os.environ["BLOB_CONTAINER_NAME"] = os.getenv("BLOB_CONTAINER_NAME")
        os.environ["BLOB_CONN_STRING"] = os.getenv("BLOB_CONN_STRING")

        # Check that all required environment variables are set
        assert os.environ["KEY_VAULT_URI"], "KEY_VAULT_URI is not set!"
        assert os.environ["BLOB_STORAGE_URL"], "BLOB_STORAGE_URL is not set!"
        assert os.environ["BLOB_CONTAINER_NAME"], "BLOB_CONTAINER_NAME is not set!"
        assert os.environ["BLOB_CONN_STRING"], "BLOB_CONN_STRING is not set!"

    def test_main_success(self):
        """Test the main function for successful execution"""
        req = HttpRequest(
            method=self.method,
            url=self.url,
            headers=self.headers,
            params=self.params,
            body=self.body
        )

        # Call the main function with the mock request
        response = main(req)

        # Check the response
        self.assertEqual(response.status_code, 200)
        self.assertIn("Data ingested successfully!", response.get_body().decode())

if __name__ == '__main__':
    print(f"Connection String: {os.getenv('BLOB_CONN_STRING')}")

    unittest.main()
