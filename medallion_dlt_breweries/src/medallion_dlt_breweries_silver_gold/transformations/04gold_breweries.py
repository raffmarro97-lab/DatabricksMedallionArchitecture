from pyspark import pipelines as dp
from pyspark.sql.functions import col

catalog = spark.conf.get("breweries.catalog")
schema_name = spark.conf.get("breweries.schema")

silver_table = f"{catalog}.{schema_name}.silver_breweries"

# Please edit the sample below


@dp.table
def silver_breweries_stream():
    return (spark.readStream
            .option("skipChangeCommits", "true")
            .table(silver_table)
            )


dp.create_streaming_table(
    name="gold_breweries",
    comment="SCD Type 2 - Storico delle modifiche"
)

dp.create_auto_cdc_flow(
    target="gold_breweries",
    source="silver_breweries_stream",
    keys=["id"],
    sequence_by=col("ingestion_ts"),
    stored_as_scd_type=2,
    except_column_list=["ingestion_ts"]
)
