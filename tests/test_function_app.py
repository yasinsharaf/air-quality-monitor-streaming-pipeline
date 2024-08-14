import sys
import os
import unittest
import requests  # Ensure requests is imported
from src.utils.ingestion_utils import (
    FetchSecret,
    get_geocoding,
    get_current_weather,
    get_three_hour_forecast
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

        # Check that all required environment variables are set
        assert self.key_vault_uri, "KEY_VAULT_URI is not set!"
        assert self.city_name, "CITY_NAME is not set!"
        assert self.state_code, "STATE_CODE is not set!"
        assert self.country_code, "COUNTRY_CODE is not set!"
        assert self.latitude, "LATITUDE is not set!"
        assert self.longitude, "LONGITUDE is not set!"

        # Fetch the API key for OpenWeather from Azure Key Vault
        self.api_key = FetchSecret("OpenWeatherAPIKeyRay")
        print(f"API Key Retrieved: {self.api_key is not None}")

    def test_get_geocoding(self):
        """Test the get_geocoding function with real data"""
        try:
            data = get_geocoding(self.api_key, self.city_name, self.state_code, self.country_code)
            print("Geocoding Data:", data)
            self.assertIn("lat", data[0])
            self.assertIn("lon", data[0])
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
        """Test the get_current_weather function with real data"""
        try:
            data = get_current_weather(self.api_key, self.latitude, self.longitude)
            print("Current Weather Data:", data)
            self.assertIn("coord", data)
            self.assertIn("weather", data)
            self.assertIn("main", data)
        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error occurred: {e}")
            raise

    def test_get_three_hour_forecast(self):
        """Test the get_three_hour_forecast function with real data"""
        try:
            data = get_three_hour_forecast(self.api_key, self.latitude, self.longitude)
            print("3-Hour Forecast Data:", data)
            self.assertIn("list", data)
            self.assertGreater(len(data["list"]), 0)  # Ensure there's at least one forecast entry
        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error occurred: {e}")
            raise

    def test_combined_data_ingestion(self):
        """Test the combined data ingestion process"""
        try:
            # Fetch geocoding data
            geocoding = get_geocoding(self.api_key, self.city_name, self.state_code, self.country_code)
            print("Geocoding Data:", geocoding)
            self.assertIn("lat", geocoding[0])
            self.assertIn("lon", geocoding[0])

            # Fetch current weather data
            current_weather = get_current_weather(self.api_key, self.latitude, self.longitude)
            print("Current Weather Data:", current_weather)
            self.assertIn("coord", current_weather)
            self.assertIn("weather", current_weather)
            self.assertIn("main", current_weather)

            # Fetch 3-hour forecast data
            forecast = get_three_hour_forecast(self.api_key, self.latitude, self.longitude)
            print("3-Hour Forecast Data:", forecast)
            self.assertIn("list", forecast)
            self.assertGreater(len(forecast["list"]), 0)  # Ensure there's at least one forecast entry

            # Combine all data
            combined_data = {
                "geocoding": geocoding,
                "current_weather": current_weather,
                "forecast": forecast
            }
            print("Combined Data:", combined_data)

            # You might also want to add tests for processing or saving this combined data if applicable

        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error occurred: {e}")
            raise
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            raise

if __name__ == '__main__':
    unittest.main()
