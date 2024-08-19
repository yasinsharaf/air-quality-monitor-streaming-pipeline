import json
import sys
import os
import unittest
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()
import requests  # Ensure requests is imported
from azure.storage.blob import BlobServiceClient
from src.utils.ingestion_utils import (
    FetchSecret,
    get_geocoding,
    get_current_weather,
    get_three_hour_forecast,
    get_air_pollution
)

# Add the src directory to the system path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

class TestAPIRealCalls(unittest.TestCase):

    def setUp(self):
        """Set up test variables and ensure environment variables are set"""
        # Fetch environment variables
        self.key_vault_uri = os.getenv("KEY_VAULT_URI")
        self.city_name = os.getenv("CITY_NAME")
        self.state_code = os.getenv("STATE_CODE")
        self.country_code = os.getenv("COUNTRY_CODE")
        self.latitude = os.getenv("LATITUDE")
        self.longitude = os.getenv("LONGITUDE")
        self.blob_conn_string = os.getenv("BLOB_CONN_STRING")
        self.blob_container_name = os.getenv("BLOB_CONTAINER_NAME")

        self.current_datetime = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

        # Check that all required environment variables are set
        assert self.key_vault_uri, "KEY_VAULT_URI is not set!"
        assert self.city_name, "CITY_NAME is not set!"
        assert self.state_code, "STATE_CODE is not set!"
        assert self.country_code, "COUNTRY_CODE is not set!"
        assert self.latitude, "LATITUDE is not set!"
        assert self.longitude, "LONGITUDE is not set!"
        assert self.blob_conn_string, "BLOB_CONN_STRING is not set!"
        assert self.blob_container_name, "BLOB_CONTAINER_NAME is not set!"

        # Fetch the API key for OpenWeather from Azure Key Vault
        self.api_key = FetchSecret("OpenWeatherAPIKeyRay")
        print(f"API Key Retrieved: {self.api_key is not None}")

        # Initialize BlobServiceClient
        self.blob_service_client = BlobServiceClient.from_connection_string(self.blob_conn_string)
        self.blob_container_client = self.blob_service_client.get_container_client(self.blob_container_name)

    def upload_to_blob(self, data, file_name):
        """Upload JSON data to Azure Blob Storage as an individual file"""
        blob_client = self.blob_container_client.get_blob_client(file_name)
        blob_client.upload_blob(data, overwrite=True)
        print(f"Uploaded {file_name} to blob storage.")

    def test_get_geocoding(self):
        """Test the get_geocoding function with real data and upload the result"""
        try:
            data = get_geocoding(self.api_key, self.city_name, self.state_code, self.country_code)
            print("Geocoding Data:", data)
            self.assertIn("lat", data[0])
            self.assertIn("lon", data[0])

            # Upload the geocoding data to the appropriate folder
            file_name = f"geocode/Batch-GeocodingAPI/geocoding.json"
            self.upload_to_blob(json.dumps(data), file_name)

        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error occurred: {e}")
            raise
        except IndexError as e:
            print(f"Index Error occurred: {e}")
            raise
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            raise

    def test_get_current_weather(self):
        """Test the get_current_weather function with real data and upload the result"""
        try:
            data = get_current_weather(self.api_key, self.latitude, self.longitude)
            print("Current Weather Data:", data)
            self.assertIn("coord", data)
            self.assertIn("weather", data)
            self.assertIn("main", data)

            # Upload the current weather data to the appropriate folder
            file_name = f"weather/{self.city_name}/RT-CurrentAPI/{self.city_name}_current_weather_{self.current_datetime}.json"
            self.upload_to_blob(json.dumps(data), file_name)

        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error occurred: {e}")
            raise

    def test_get_three_hour_forecast(self):
        """Test the get_three_hour_forecast function with real data and upload the result"""
        try:
            data = get_three_hour_forecast(self.api_key, self.latitude, self.longitude)
            print("3-Hour Forecast Data:", data)
            self.assertIn("list", data)
            self.assertGreater(len(data["list"]), 0)  # Ensure there's at least one forecast entry

            # Upload the forecast data to the appropriate folder
            file_name = f"weather/{self.city_name}/RT-3HrForecastAPI/{self.city_name}_three_hour_forecast_{self.current_datetime}.json"
            self.upload_to_blob(json.dumps(data), file_name)

        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error occurred: {e}")
            raise

    def test_get_air_pollution(self):
        """Test the get_air_pollution function with real data and upload the result"""
        try:
            data = get_air_pollution(self.api_key, self.latitude, self.longitude)
            print("Air Pollution Data:", data)
            self.assertIn("list", data)
            self.assertGreater(len(data["list"]), 0)  # Ensure there's at least one pollution entry

            # Upload the air pollution data to the appropriate folder
            file_name = f"pollution/{self.city_name}/{self.city_name}_air_pollution_{self.current_datetime}.json"
            self.upload_to_blob(json.dumps(data), file_name)

        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error occurred: {e}")
            raise            
if __name__ == '__main__':
    city_name = os.getenv("CITY_NAME")
    state_code = os.getenv("STATE_CODE")
    country_code = os.getenv("COUNTRY_CODE")
    print(f"City Name: {city_name}, State Code: {state_code}, Country Code: {country_code}")
    unittest.main()

    

