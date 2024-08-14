import requests  # for making HTTP requests to APIs
from azure.identity import ManagedIdentityCredential, AzureCliCredential  # for working with Azure Managed Identity credentials
from azure.keyvault.secrets import SecretClient  # for retrieving secrets from Azure Key Vault
from dotenv import load_dotenv  # for loading environment variables from a .env file
import os  # for accessing environment variables

# Load environment variables from the .env file
load_dotenv()

def FetchSecret(secret_name):
    """
    Fetches a secret from Azure Key Vault using a User Assigned Managed Identity (UAMI) by using Key Vault URI.

    Args:
        secret_name (str): The name of the secret to fetch from the Key Vault.

    Returns:
        str: The secret value stored in Key Vault

    Raises:
        azure.core.exceptions.HttpResponseError: If the secret retrieval fails due to an HTTP error.
    """

    if os.getenv("AZURE_FUNCTIONS_ENVIRONMENT") == "Development":
        credential = AzureCliCredential()
    else:
        credential = ManagedIdentityCredential(client_id=os.getenv("UAMI_CLIENT_ID"))
    
    kv_url = os.getenv("KEY_VAULT_URI")
    client = SecretClient(vault_url=kv_url, credential=credential)
    secret = client.get_secret(secret_name)
    return secret.value

def call_api(base_url, params):
    """Generic API caller."""
    response = requests.get(base_url, params=params)
    response.raise_for_status()  # Raise an error on bad status
    return response.json()

def get_geocoding(api_key, city_name, state_code=None, country_code=None):
    """Fetch geocoding information."""
    base_url = "http://api.openweathermap.org/geo/1.0/direct"
    location = city_name
    if state_code:
        location += f",{state_code}"
    if country_code:
        location += f",{country_code}"

    params = {'q': location, 'appid': api_key, 'limit': 1}
    return call_api(base_url, params)

def get_current_weather(api_key, lat=44.34, lon=10.99):
    """Fetch current weather data."""
    base_url = "https://api.openweathermap.org/data/2.5/weather"
    params = {'lat': lat, 'lon': lon, 'appid': api_key}
    return call_api(base_url, params)


def get_three_hour_forecast(api_key, lat, lon):
    """Fetch 3-hour forecast data (up to 5 days)."""
    base_url = "https://api.openweathermap.org/data/2.5/forecast"
    params = {'lat': lat, 'lon': lon, 'appid': api_key}
    return call_api(base_url, params)