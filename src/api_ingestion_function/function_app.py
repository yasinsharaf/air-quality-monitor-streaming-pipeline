import azure.functions as func
import datetime
import json
import logging
import os

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient
from utils.ingestion_utils import FetchSecret, OpenWeatherAPIGeocoding

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        vault_url = os.getenv("KEY_VAULT_URI")
        api_key = FetchSecret("OpenWeatherAPIKeyYasin")

        geocoding = OpenWeatherAPIGeocoding(api_key=api_key)

        #convert data --> json
        geocoding_json = json.dumps(geocoding)

        #connect to blob storage
        account_url = os.getenv("BLOB_STORAGE_URL")
        container_name = os.getenv("BLOB_CONTAINER_NAME")
        conn_string = os.getenv("BLOB_CONN_STRING")

        blob_service_client = BlobServiceClient.from_connection_string(conn_string)
        #specify name of blob file as param
        blob_client_geocoding = blob_service_client.get_blob_client(container=container_name, blob="geocoding_info.json")

        #upload to blob storage
        blob_client_geocoding.upload_blob(data=geocoding_json, overwrite=True, blob_type="BlockBlob")

        return func.HttpResponse("Data ingested successfully!", status_code=200)

    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)


