import requests                                                     # for requesting response from api call
from azure.identity import ManagedIdentityCredential, AzureCliCredential, DefaultAzureCredential                # work with your UAMI credentials
from azure.keyvault.secrets import SecretClient                     # for retrieving keys from Key Vault
from dotenv import load_dotenv                                      # for loading environment variables (usernames passwords urls etc) stored in .env file
import os                                                           # for using environment variables once loaded in different files, os.getenv()

# load env vars
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

    Example:
        secret_value = FetchSecret("mySecretName")
    """
    credential = ManagedIdentityCredential(client_id=os.getenv("UAMI_CLIENT_ID"))
    kv_url = os.getenv("KEY_VAULT_URI")
    client = SecretClient(vault_url=kv_url, credential=credential)
    secret = client.get_secret(secret_name)
    return secret.value

def OpenWeatherAPIGeocoding(api_key, city_name, state_code=None, country_code=None):
    """
    Retrieves geocoding information for a specified city using the OpenWeather API.
    
    Parameters:
    - api_key (str): Your OpenWeather API key.
    - city_name (str): The name of the city you want to geocode.
    - state_code (str, optional): The state code for the city. Default is None.
    - country_code (str, optional): The country code for the city. Default is None.
    
    Returns:
    - dict: The API response containing geocoding information.
    """
    
    base_url = "http://api.openweathermap.org/geo/1.0/direct?"

    location = city_name
    if state_code:
        location += f",{state_code}"
    if country_code:
        location += f",{country_code}"

    params = {
        'q': location,
        'appid': api_key,
        'limit': 10
    }

    response = requests.get(base_url, params = params)
    return response

