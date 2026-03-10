from pyspark import pipelines as dp
from pyspark.sql.functions import col


# Please edit the sample below
@dp.table
def silver_breweries_stream():
    return spark.readStream.table("silver_breweries")

dp.create_streaming_table(
    name = "workspace.pipeline_breweries.gold_breweries",
    comment = "SCD Type 2 - Storico delle modifiche"
)

dp.create_auto_cdc_flow(
    target = "gold_breweries",
    source = "silver_breweries_stream",
    keys = ["brewery_sk"],
    sequence_by = col("ingestion_ts"),
    stored_as_scd_type = 2,
    except_column_list = ["ingestion_ts"]
)