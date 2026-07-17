from pyspark import pipelines as dp
from pyspark.sql.functions import col, count, count_if
from pyspark.sql import functions as F
#from utilities import utils

catalog = spark.conf.get("breweries.catalog")
schema_name = spark.conf.get("breweries.schema")

bronze_table = f"{catalog}.{schema_name}.bronze_breweries"
cdc_table = f"{catalog}.{schema_name}.cdc_breweries"

@dp.table(
    name = "workspace.pipeline_breweries.silver_staging_breweries",
    comment = "Add ingestion_ts"
)
def silver_staging_breweries():
    api_df = spark.read.table(bronze_table)
    cdc_df = spark.read.table(cdc_table)
    
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
                            how = "left_anti"
                        )
    
    return api_filtered.unionByName(cdc_df)