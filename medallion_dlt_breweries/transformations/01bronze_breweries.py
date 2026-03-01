from pyspark import pipelines as dp
from pyspark.sql.functions import col, count, count_if
from pyspark.sql import functions as F
from utilities import utils
import requests

# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.

@dp.table(
  name = "bronze_breweries",
  comment = "Raw API Ingestion"
)
def bronze_breweries():
    api = "https://api.openbrewerydb.org/v1/breweries?per_page=200"
    response = requests.get(api)

    run_ts = spark.sql("SELECT current_timestamp()").collect()[0][0]

    data = response.json()
    SparkSession.builder.appName("Breweries").getOrCreate()
    df_raw = spark.createDataFrame(data)
    return df_raw