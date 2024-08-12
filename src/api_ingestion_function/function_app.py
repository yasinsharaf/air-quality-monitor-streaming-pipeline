

# Importing necessary Azure Functions module
import azure.functions as func

# Importing datetime to work with dates and times
import datetime

# Importing json to work with JSON data
import json

# Importing logging to log information and errors
import logging

# Importing os to access environment variables
import os

# Importing DefaultAzureCredential for Azure authentication
from azure.identity import DefaultAzureCredential

# Importing SecretClient to access secrets from Azure Key Vault
from azure.keyvault.secrets import SecretClient

# Importing BlobServiceClient to interact with Azure Blob Storage
from azure.storage.blob import BlobServiceClient

# Importing custom utility functions
from utils.ingestion_utils import FetchSecret, OpenWeatherAPIGeocoding

import azure.functions as func
from azure.functions import HttpRequest
import json

# Your existing imports and code here

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        # Retrieve the Key Vault URI from environment variables
        vault_url = os.getenv("KEY_VAULT_URI")
        print(f"Vault URL: {vault_url}")

        # Fetch the API key for OpenWeather from the Key Vault using a custom utility function
        api_key = FetchSecret("OpenWeatherAPIKeyYasin")
        print(f"API Key Retrieved: {api_key is not None}")

        # Example city name, state code, and country code
        city_name = "New York"
        state_code = "NY"
        country_code = "US"

        # Fetch the actual geocoding data from OpenWeather
        geocoding = OpenWeatherAPIGeocoding(api_key=api_key, city_name=city_name, state_code=state_code, country_code=country_code)
        print("Geocoding data retrieved")

        # Convert the geocoding data to a JSON string
        geocoding_json = json.dumps(geocoding)
        print(f"Geocoding JSON: {geocoding_json[:100]}")  # Print first 100 characters of JSON

        # Retrieve the Blob Storage account URL, container name, and connection string from environment variables
        account_url = os.getenv("BLOB_STORAGE_URL")
        container_name = os.getenv("BLOB_CONTAINER_NAME")
        conn_string = os.getenv("BLOB_CONN_STRING")
        print(f"Account URL: {account_url}, Container Name: {container_name}, Connection String Retrieved: {conn_string is not None}")

        # Create a BlobServiceClient using the connection string
        blob_service_client = BlobServiceClient.from_connection_string(conn_string)
        print("BlobServiceClient created")

        # Get a BlobClient for the specific blob where geocoding data will be stored
        blob_client_geocoding = blob_service_client.get_blob_client(
            container=container_name, 
            blob="geocoding_info.json"
        )
        print("BlobClient for geocoding_info.json created")

        # Upload the JSON geocoding data to the specified blob in Azure Blob Storage
        blob_client_geocoding.upload_blob(
            data=geocoding_json, 
            overwrite=True, 
            blob_type="BlockBlob"
        )
        print("Geocoding JSON uploaded to Blob Storage")

        # Return an HTTP response indicating successful data ingestion
        return func.HttpResponse("Data ingested successfully!", status_code=200)

    except Exception as e:
        # Log the error message for debugging purposes
        logging.error(f"Error: {str(e)}")
        print(f"Error occurred: {str(e)}")
        
        # Return an HTTP response indicating the error with a 500 status code
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)