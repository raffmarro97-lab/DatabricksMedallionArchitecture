from pyspark import pipelines as dp
from pyspark.sql.functions import col, count, count_if
from pyspark.sql import functions as F
#from utilities import utils
from pyspark.sql.types import *
import requests

catalog = spark.conf.get("breweries.catalog")
schema_name = spark.conf.get("breweries.schema")

config_table = f"{catalog}.{schema_name}.bronze_breweries_job_config"
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
    StructField("longitude",      DoubleType(), True),
    StructField("latitude",       DoubleType(), True),
    StructField("phone",          StringType(), True),
    StructField("website_url",    StringType(), True),
    StructField("state",          StringType(), True),
    StructField("street",         StringType(), True)
  ]
)

@dp.table(
  name="bronze_breweries",
  comment = "Raw API Ingestion"
)
def bronze_breweries():
  
  per_page = 10

  config_row = (
      spark.table(config_table)
        .select("current_page")
        .collect()[0]
  )

  current_page = config_row["current_page"]

  params = {
    "page": current_page,
    "per_page": per_page
  }

  api = "https://api.openbrewerydb.org/v1/breweries"
  
  # Legge il valore passato dal notebook precedente
  response = requests.get(api, params = params, timeout = 30)
  
  response.raise_for_status()

  data = response.json()

  if data:
    df_raw = spark.createDataFrame(data, schema = empty_schema)
    return df_raw
  else:
    return spark.createDataFrame([], schema = empty_schema)