# Databricks notebook source
import requests       
import json                             
from pyspark.sql import Row
from pyspark.sql.types import *
from geopy.geocoders import AzureMaps
from datetime import datetime as dt

# COMMAND ----------

# MAGIC %md
# MAGIC ### Water Quality

# COMMAND ----------

# MAGIC %md
# MAGIC #### Functions

# COMMAND ----------

# MAGIC %md
# MAGIC For fetching geocodes of cities passed in, the API response from Meersens for water pollution, and extracting relevant JSON objects for the spark DF

# COMMAND ----------


def fetch_geocodes(city, country, address=None):
    """
    Fetches the latitude and longitude for a given city, country, and optional address.

    Parameters:
    city (str): The city name.
    country (str): The country name.
    address (str, optional): An optional detailed address within the city. Defaults to None.

    Returns:
    tuple: A tuple containing the latitude and longitude as floats (lat, lng).

    Example:
    lat, lng = fetch_geocodes("Paris", "France", "123 Main St")
    print(lat, lng)  # Outputs the latitude and longitude for the given address in Paris, France.
    """
    location = geocoder.geocode(f"{address}, {city}, {country}")
    lat = location.latitude
    lng = location.longitude
    return lat, lng


# COMMAND ----------

def fetch_water_quality(lat, lng, api_key):
    """
    Sends a request to the Meersens API to retrieve real-time water quality data based on latitude and longitude coordinates.

    Args:
        lat (float): The latitude of the location for which water quality data is requested.
        lng (float): The longitude of the location for which water quality data is requested.
        api_key (str): The API key required for authenticating the request to the Meersens API.

    Returns:
        str: The API response as a JSON-formatted string if the request is successful (status code 200).
        int: The HTTP status code if the request fails (non-200 response).

    Example:
        api_key = "your_api_key"
        lat, lng = 48.8566, 2.3522  # Coordinates for Paris, France
        result = fetch_water_quality(lat, lng, api_key)
        
        if isinstance(result, str):
            print("API response received:", result)
        else:
            print(f"Failed to fetch data, status code: {result}")

    Raises:
        None: The function returns the status code if the request fails, allowing for error handling in the calling function.
    """
    url = "https://api.meersens.com/environment/public/water/current"
    headers = {
        'apikey': api_key
    }
    params = {
        "lat": lat,
        "lng": lng,
        "health_recommendations": True
    }
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        return json.dumps(response.json())  # Convert the response JSON into a string
    else:
        return response.status_code


# COMMAND ----------

def extract_data_from_response(response):
    """
    Extracts the most critical information from the JSON response of the Meersens water quality API.

    Args:
        response (str): A JSON-formatted string returned by the API containing water quality data.

    Returns:
        tuple: A tuple containing:
            - found (bool): Whether at least one pollutant value has been found.
            - datetime (str): The ISO_8601 UTC datetime indicating the time the data refers to.
            - index_name (str): The name of the returned water quality index.
            - qualification (str): A textual representation of the water quality (e.g., "Good", "Moderate").
            - description (str): A more detailed textual assessment of the water quality.
            - color (str): A hexadecimal color code representing the water quality level.
            - health_recommendations (str): General health recommendations for the population, if available.
            - response (str): The original JSON response as a string for reference or future processing.
    
    Example:
        response = '{"found": true, "datetime": "2024-09-10T05:00:00.000Z", "index": {...}, "health_recommendations": {...}}'
        found, datetime, index_name, qualification, description, color, health_recommendations, original_response = extract_data_from_response(response)
    """
    # Parse the JSON response
    data = json.loads(water_quality_data)

    # Extract critical information
    found = data.get('found', False)
    datetime = data.get('datetime', None)

    index = data.get('index', {})
    index_name = index.get('index_name', None)
    qualification = index.get('qualification', None)
    description = index.get('description', None)
    color = index.get('color', None)

    # Health recommendations (optional)
    health_recommendations = data.get('health_recommendations', {}).get('all', None)

    # Return extracted information and original JSON
    return found, datetime, index_name, qualification, description, color, health_recommendations, response


# COMMAND ----------

# MAGIC %md
# MAGIC #### Obtain city coordinates

# COMMAND ----------


#set cities
cities = ["Paris", "Nice", "Bordeaux"]
country="France"

#configure geocoder to fetch coordinates from Azure Maps API
maps_key = "your-azure-maps-key"
geocoder = AzureMaps(subscription_key=maps_key)

#store output in dict if they exist, and store coords as tuples
city_geocodes = {}
for city in cities:
    lat, lng = fetch_geocodes(city, country)
    if lat is not None and lng is not None:
        key = f"{city}, {country}"
        city_geocodes[key] = (lat, lng)
    else:
        print(f"Location coordinates for {city}, {country} not found")

print(city_geocodes)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Obtain API Data and Create Spark DF

# COMMAND ----------

    
meersens_api_key = "your-meersens-key"

#fetch water quality data into empty rows
rows = []
for key, (lat, lng) in city_geocodes.items():
    city, country = key.split(",")

    #get rid of whitespace " country"
    country = country.strip()
    water_quality_data = fetch_water_quality(lat, lng, meersens_api_key)

    #only fetch relevant data
    found, datetime, index_name, qualification, description, color, health_recommendations, raw_json = extract_data_from_response(response=water_quality_data)

    #convert datetime str --> data and time vars
    datetime = dt.strptime(datetime, "%Y-%m-%dT%H:%M:%S.%fZ")
    date = datetime.date()

    #create rows and name the columns for spark df
    rows.append(
        Row(
            City=city,
            Country=country,
            Lat=lat,
            Lng=lng,
            Found=found,
            Datetime=datetime,
            Date=date,
            IndexName=index_name,
            Qualification=qualification,
            Description=description,
            Color=color,
            HealthRecommendations=health_recommendations,
            RawJSON=raw_json  # Keep the full JSON response here
        )
    )
    
water_quality_df = spark.createDataFrame(rows)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write to Landing Zone

# COMMAND ----------

#connect to blob storage
storage_account_name = "your-storage-account-name"
storage_account_access_key = "your-storage-account-key"
#configure
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net", storage_account_access_key)

# COMMAND ----------

#check connection
try:
    files = dbutils.fs.ls(f"wasbs://bronze@{storage_account_name}.blob.core.windows.net/")
    print("Connection to Bronze layer successful. Files in container:")
    for file in files:
        print(file.name)
except Exception as e:
    print("Connection failed:", str(e))

# COMMAND ----------

#overwrites data in most granular partition (in this case the date)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

output_path=f"wasbs://bronze@{storage_account_name}.blob.core.windows.net/pollution/water/"
water_quality_df.write \
    .format("json") \
    .mode("overwrite") \
    .partitionBy("Country", "City", "Date") \
    .option("path", output_path) \
    .save()

