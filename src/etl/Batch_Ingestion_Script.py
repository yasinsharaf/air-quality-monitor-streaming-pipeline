# Databricks notebook source
# MAGIC %md
# MAGIC ### Water Quality Batch Ingestion

# COMMAND ----------

import requests       
import json                             
from pyspark.sql import Row
from pyspark.sql.types import *
import os

# COMMAND ----------

# MAGIC %md
# MAGIC #### Connect to Landing Zone (Bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Fetch Data From Meersens URL

# COMMAND ----------

url = "https://api.meersens.com/environment/public/water/current"
api_key = os.getenv("OPENWEATHER_API_KEY")

#partition by city or coordinates to enable parallel processing in spark
def fetch_data_for_partition(iterator):
    results = []    #store results from API
    headers = {"api_key": api_key}
    for row in iterator:
        response = requests.get(
            url,
            headers=headers,
            params = {
                "lat": row.lat,
                "lng": row.lng
            }
        )
        #store response of json data if successful connection
        #water_quality_data stores the fetched json as a string for each city
        if response.status_code == 200:
            print("API Call Successful")
            data = response.json()
            print(json.dumps(data))
            #convert response into JSON string to avoid Spark schema reading issues
            results.append(Row(name=row.name, lat=row.lat, lng=row.lng, water_quality_data=json.dumps(data)))
        else:
            results.append(Row(name=row.name, lat=row.lat, lng=row.lng, water_quality_data=f"Error: {response.status_code}"))
    return iter(results)

#params for the data you need by city
cities = [
    {"name": "Miami", "lat": 25.762, "lng": -80.192},
    {"name": "Los Angeles", "lat": 34.055, "lng": -118.243}
]

#create spark df of city coordinates
cities_df = spark.createDataFrame(cities)

schema = StructType([
    StructField("name", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("water_quality_data", StringType(), True)  # Assuming JSON data is stored as a string
])

#fetch data using mapPartitions
water_quality_rdd = cities_df.rdd.mapPartitions(fetch_data_for_partition).toDF()
first_element = water_quality_rdd.take(1)
print(first_element)


#water_quality_df = spark.createDataFrame(water_quality_rdd, schema)

#show df
#water_quality_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write to Landing Zone

# COMMAND ----------

#connect to blob storage
storage_account_name = os.getenv("BLOB_STORAGE_ACCOUNT_NAME")
storage_account_access_key = os.getenv("BLOB_CONTAINER_ACCESS_KEY")
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

output_path=f"wasbs://bronze@{storage_account_name}.blob.core.windows.net/"
checkpoint_path=f"wasbs://bronze@{storage_account_name}.blob.core.windows.net/checkpoints"

"""
water_quality_df.writeStream \
    .trigger(once=True) \
    .format("json") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_paths) \
    .partitionBy("name") \
    .outputMode("append") \
    .start()
"""
