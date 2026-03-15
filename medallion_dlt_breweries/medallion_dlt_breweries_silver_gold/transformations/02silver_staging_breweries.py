from pyspark import pipelines as dp
from pyspark.sql.functions import col, count, count_if
from pyspark.sql import functions as F
from utilities import utils

@dp.table(
    name = "workspace.pipeline_breweries.silver_staging_breweries",
    comment = "Add ingestion_ts"
)
def silver_breweries():
    api_df = spark.read.table("pipeline_breweries.bronze_breweries")
    cdc_df = spark.read.table("pipeline_breweries.cdc_breweries_events")
    
    run_ts = spark.sql("SELECT current_timestamp()").collect()[0][0]
    api_df = (
            api_df.select(            
                'address_1', 
                'address_2', 
                'address_3', 
                'brewery_type', 
                'city', 
                'country', 
                'id', 
                'latitude', 
                'longitude', 
                'name', 
                'phone', 
                'postal_code', 
                'state', 
                'state_province', 
                'street', 
                'website_url'
            )
            .withColumn(
                "ingestion_ts", 
                F.lit(run_ts) #da utlizzare come sequence_by
            )
        
        )
    
    api_filtered = api_df.join(
                            cdc_df.select("id").distinct(), 
                            on ="id", 
                            how = "leftanti"
                        )
    
    return api_filtered.unionByName(cdc_df)