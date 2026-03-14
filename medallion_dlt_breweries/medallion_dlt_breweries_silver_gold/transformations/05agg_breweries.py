from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name = "workspace.pipeline_breweries.agg_breweries",
    comment = "Aggregated breweries - Numero di breweries per stato"
)
def agg_breweries():
    df =  spark.read.table("workspace.pipeline_breweries.gold_breweries")

    return (
        df
            .where(F.col("__END_AT").isNull()) # solo record correnti SCD2
            .groupBy("state")
            .agg(
                F.count("brewery_sk").alias("num_breweries"),
                F.countDistinct("brewery_type").alias("num_types")
            )
            .orderBy(F.col("num_breweries").desc())

    )