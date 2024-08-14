import sys
import os
import datetime
import json
import logging
import azure.functions as func

# Importing Azure SDK modules
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient

# Ensure that the src/utils directory is in the system path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'src', 'utils')))

# Import your utility functions
from ingestion_utils import FetchSecret, get_geocoding, get_current_weather, get_three_hour_forecast

# Set the default encoding to UTF-8
sys.stdout.reconfigure(encoding='utf-8')

app = func.FunctionApp()

@app.function_name(name="HttpTriggerIngest")
@app.route(route="ingest", methods=["GET", "POST"], auth_level=func.AuthLevel.FUNCTION)

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        # Retrieve necessary environment variables
        vault_url = os.getenv("KEY_VAULT_URI")
        city_name = os.getenv("CITY_NAME")  # No default, should be set in the environment
        state_code = os.getenv("STATE_CODE")  # No default, should be set in the environment
        country_code = os.getenv("COUNTRY_CODE")  # No default, should be set in the environment
        blob_storage_url = os.getenv("BLOB_STORAGE_URL")
        blob_container_name = os.getenv("BLOB_CONTAINER_NAME")
        blob_conn_string = os.getenv("BLOB_CONN_STRING")
        latitude = os.getenv("LATITUDE")  # Latitude for the weather data
        longitude = os.getenv("LONGITUDE")  # Longitude for the weather data

        # Ensure all necessary environment variables are set
        if not all([vault_url, city_name, state_code, country_code, blob_storage_url, blob_container_name, blob_conn_string, latitude, longitude]):
            missing_vars = [var for var in ["vault_url", "city_name", "state_code", "country_code", "blob_storage_url", "blob_container_name", "blob_conn_string", "latitude", "longitude"] if not eval(var)]
            raise ValueError(f"Missing environment variables: {', '.join(missing_vars)}")

        print(f"Vault URL: {vault_url}")
        print(f"City: {city_name}, State: {state_code}, Country: {country_code}")
        print(f"Blob Storage URL: {blob_storage_url}, Container: {blob_container_name}")
        print(f"Blob Connection String Retrieved: {blob_conn_string is not None}")

        # Fetch the API key from Azure Key Vault
        api_key = FetchSecret("OpenWeatherAPIKeyRay")
        print(f"API Key Retrieved: {api_key is not None}")

        # Fetch geocoding data
        geocoding = get_geocoding(api_key=api_key, city_name=city_name, state_code=state_code, country_code=country_code)
        print(f"Geocoding data retrieved: {geocoding[:100]}")  # Print first 100 characters of JSON

        # Fetch current weather data
        current_weather = get_current_weather(api_key=api_key, lat=latitude, lon=longitude)
        print(f"Current weather data retrieved: {json.dumps(current_weather)[:100]}")  # Print first 100 characters

        # Fetch 3-hour forecast data
        forecast = get_three_hour_forecast(api_key=api_key, lat=latitude, lon=longitude)
        print(f"3-hour forecast data retrieved: {json.dumps(forecast)[:100]}")  # Print first 100 characters

        # Prepare the data to be uploaded (e.g., combining all data into one JSON object)
        combined_data = {
            "geocoding": geocoding,
            "current_weather": current_weather,
            "forecast": forecast
        }

        # Convert the combined data to a JSON string
        combined_json = json.dumps(combined_data)
        print(f"Combined JSON: {combined_json[:100]}")  # Print first 100 characters

        # Create a BlobServiceClient using the connection string
        blob_service_client = BlobServiceClient.from_connection_string(blob_conn_string)
        print("BlobServiceClient created")

        # Get a BlobClient for the specific blob
        blob_client = blob_service_client.get_blob_client(container=blob_container_name, blob="weather_data.json")
        print("BlobClient for weather_data.json created")

        # Upload the combined JSON data to the specified blob
        blob_client.upload_blob(data=combined_json, overwrite=True, blob_type="BlockBlob")
        print("Combined JSON uploaded to Blob Storage")

        # Return an HTTP response indicating successful data ingestion
        return func.HttpResponse("Data ingested successfully!", status_code=200)

    except Exception as e:
        logging.error(f"Error: {str(e)}")
        print(f"Error occurred: {str(e)}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)

