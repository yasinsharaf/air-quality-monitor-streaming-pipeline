# Databricks notebook source
# MAGIC %md
# MAGIC ### Connection to Bronze and Silver Containers

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

#check connection
try:
    files = dbutils.fs.ls(f"wasbs://silver@{storage_account_name}.blob.core.windows.net/")
    print("Connection to Silver layer successful. Files in container:")
    for file in files:
        print(file.name)
except Exception as e:
    print("Connection failed:", str(e))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Dataframe as a Stream

# COMMAND ----------

# import necessary packages for creating Spark session, schema def, and transform
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# schema
schema = StructType([
    StructField("City", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Lat", DoubleType(), True),
    StructField("Lng", DoubleType(), True),
    StructField("Found", BooleanType(), True),
    StructField("Datetime", TimestampType(), True),
    StructField("Date", DateType(), True),
    StructField("IndexName", StringType(), True),
    StructField("Qualification", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Color", StringType(), True),
    StructField("MainPollutant", ArrayType(StringType()), True),  # List of main pollutants
    StructField("HealthRecommendations", StringType(), True),
    StructField("Pollutants", ArrayType(StructType([   # Nested struct for pollutants
        StructField("name", StringType(), True),
        StructField("shortcode", StringType(), True),
        StructField("unit", StringType(), True),
        StructField("value", FloatType(), True),
        StructField("confidence", FloatType(), True),
        StructField("qualification", StringType(), True),
        StructField("color", StringType(), True),
        StructField("description", StringType(), True)
    ])), True),
    StructField("RawJSON", StringType(), True)
])

# COMMAND ----------

streaming_df = spark.readStream \
    .schema(schema) \
    .json(f"wasbs://bronze@{storage_account_name}.blob.core.windows.net/pollution/water/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC #### Filtering Data and Dropping Columns

# COMMAND ----------

from pyspark.sql import functions as func

# COMMAND ----------

# keep only those records that have found water quality data
water_transformed_df = streaming_df.filter(col("Found") == True)              

# COMMAND ----------

# only keep pollutant data that is highly reliable (4) or very highly reliable (5) for an accurate dashboard before exploding to save on compute downstream
water_transformed_df = water_transformed_df.withColumn("Pollutants", func.expr("filter(Pollutants, p  -> p.confidence >= 4)"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Exploding Arrays

# COMMAND ----------

# explode array of main pollutants and pollutants
water_transformed_df =  water_transformed_df.withColumn("MainPollutant", func.explode("MainPollutant"))
water_transformed_df =  water_transformed_df.withColumn("Pollutant", func.explode("Pollutants"))

water_transformed_df = water_transformed_df.select(
    "City",
    "Country",
    "Date",
    "Lat",
    "Lng",
    func.col("Qualification").alias("WaterQualification"),
    "Color",
    "Description",
    "MainPollutant",
    func.col("Pollutant.name").alias("PollutantName"),
    func.col("Pollutant.value").alias("PollutantLevel"),
    func.col("Pollutant.unit").alias("PollutantMeasurementUnit"),
    func.col("Pollutant.qualification").alias("PollutantQualification"),
    "HealthRecommendations"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Stream

# COMMAND ----------

water_destination = f"wasbs://silver@{storage_account_name}.blob.core.windows.net/enriched-pollution/water/"
water_checkpoint = f"wasbs://silver@{storage_account_name}.blob.core.windows.net/checkpoints/enriched-pollution-checkpoints/water-checkpoints/"

# Explicitly print out the paths to verify they are correct
print("Water Destination Path:", water_destination)
print("Water Checkpoint Path:", water_checkpoint)

query = water_transformed_df.writeStream \
    .format("parquet") \
    .partitionBy("Date", "Country", "City") \
    .option("path", water_destination) \
    .option("checkpointLocation", water_checkpoint) \
    .trigger(once=True) \
    .start()

#keep the stream active until manual exit
query.awaitTermination()
