from pyspark import pipelines as dp
from medallion_dlt_breweries_silver_gold.transformations.silver_transformations import (
    transform_silver_breweries,
)

# Parametri ricevuti da silver_gold.pipeline.yml
catalog = spark.conf.get("breweries.catalog")
schema_name = spark.conf.get("breweries.schema")

silver_staging_table = f"{catalog}.{schema_name}.silver_staging_breweries"

@dp.table(
    name="silver_breweries",
    comment="Cleaning the bronze table, and add ingestion_ts"
)
def silver_breweries():
    source_df = spark.read.table(silver_staging_table)
    return transform_silver_breweries(source_df)