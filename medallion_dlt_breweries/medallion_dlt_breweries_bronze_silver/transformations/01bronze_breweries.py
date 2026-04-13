from pyspark import pipelines as dp
from pyspark.sql.functions import col, count, count_if
from pyspark.sql import functions as F
from utilities import utils
from pyspark.sql.types import *
import requests

# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.
empty_schema = StructType(
  [
    StructField("id",             StringType(), True),
    StructField("name",           StringType(), True),
    StructField("brewery_type",   StringType(), True),
    StructField("address_1",      StringType(), True),
    StructField("address_2",      StringType(), True),
    StructField("address_3",      StringType(), True),
    StructField("city",           StringType(), True),
    StructField("state_province", StringType(), True),
    StructField("postal_code",    StringType(), True),
    StructField("country",        StringType(), True),
    StructField("longitude",      StringType(), True),
    StructField("latitude",       StringType(), True),
    StructField("phone",          StringType(), True),
    StructField("website_url",    StringType(), True),
    StructField("state",          StringType(), True),
    StructField("street",         StringType(), True)
  ]
)

@dp.table(
  name="workspace.pipeline_breweries.bronze_breweries",
  comment = "Raw API Ingestion"
)
def bronze_breweries():
  
  per_page = 10

  current_page = int(spark.conf.get("current_page", "1"))

  params = {
    "page": current_page,
    "per_page": per_page
  }

  api = "https://api.openbrewerydb.org/v1/breweries"
  
  # Legge il valore passato dal notebook precedente
  response = requests.get(api, params = params)
  
  data = response.json()

  if data:
    df_raw = spark.createDataFrame(data)
    return df_raw
  else:
    return spark.createDataFrame([], schema = empty_schema)