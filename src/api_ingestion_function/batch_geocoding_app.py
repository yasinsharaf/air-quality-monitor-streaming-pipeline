import sys
import os
import json
import logging
from datetime import datetime
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient

# Ensure that the src/utils directory is in the system path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'src', 'utils')))
from ingestion_utils import FetchSecret, get_geocoding

# Set the default encoding to UTF-8
sys.stdout.reconfigure(encoding='utf-8')

app = func.FunctionApp()

@app.function_name(name="HttpTriggerIngest")
@app.route(route="ingest", methods=["GET", "POST"], auth_level=func.AuthLevel.FUNCTION)
def azure_function_handler(req: func.HttpRequest) -> func.HttpResponse:
    try:
        message = process_request()
        return func.HttpResponse(message, status_code=200)
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)

def process_request():
    logging.info('Processing request...')

    # Retrieve necessary environment variables
    vault_url = os.getenv("KEY_VAULT_URI")
    city_name = os.getenv("CITY_NAME")  # No default, should be set in the environment
    blob_storage_url = os.getenv("BLOB_STORAGE_URL")
    blob_container_name = "bronze"
    blob_conn_string = os.getenv("BLOB_CONN_STRING")

    # Get the current date and time
    current_datetime = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    current_datet = datetime.now().strftime('%Y-%m-%d')

    # Ensure all necessary environment variables are set
    missing_vars = [var for var in ["vault_url", "city_name", "blob_storage_url", "blob_container_name", "blob_conn_string"] if not locals()[var]]
    if missing_vars:
        raise ValueError(f"Missing environment variables: {', '.join(missing_vars)}")

    logging.info(f"Vault URL: {vault_url}")
    logging.info(f"City: {city_name}")
    logging.info(f"Blob Storage URL: {blob_storage_url}, Container: {blob_container_name}")
    logging.info(f"Blob Connection String Retrieved: {blob_conn_string is not None}")

    # Fetch the API key from Azure Key Vault
    api_key = FetchSecret("OpenWeatherAPIKeyYasin")
    logging.info(f"API Key Retrieved: {api_key is not None}")

    # Create a BlobServiceClient using the connection string
    blob_service_client = BlobServiceClient.from_connection_string(blob_conn_string)
    logging.info("BlobServiceClient created")

    # Fetch geocoding data
    geocoding = get_geocoding(api_key=api_key, city_name=city_name)
    logging.info(f"Geocoding data retrieved: {geocoding[:100]}")  # Print first 100 characters of JSON

    # Upload geocoding data to the "geocode" folder
    geocoding_file_name = f"geocode/Batch-GeocodingAPI/{city_name}_geocoding.json"
    blob_client = blob_service_client.get_blob_client(container=blob_container_name, blob=geocoding_file_name)
    blob_client.upload_blob(data=json.dumps(geocoding), overwrite=True)
    logging.info(f"Geocoding data uploaded as {geocoding_file_name}")

    return "Data ingested successfully!"

if __name__ == "__main__":
    try:
        message = process_request()
        print(message)
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        print(f"Error occurred: {str(e)}")
