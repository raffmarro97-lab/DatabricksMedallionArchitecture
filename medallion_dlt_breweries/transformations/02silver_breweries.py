from pyspark import pipelines as dp
from pyspark.sql.functions import col, count, count_if
from pyspark.sql import functions as F
from utilities import utils

@dp.table(
    name = "workspace.pipeline_breweries.silver_breweries",
    comment = "Cleaning the bronze table,  and add ingestion_ts"
)
def silver_breweries():
    df = spark.read.table("bronze_breweries")
    run_ts = spark.sql("SELECT current_timestamp()").collect()[0][0]
    df = (
            df.select(            
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
            .where(
                ( F.col("country") == "United States") &
                ( F.col("address_1").isNotNull() )
            )
            .withColumn(
                "address_2",
                utils.fill_null("address_2", "Doesn't exist")
            )
            .withColumn(
                "address_3",
                utils.fill_null("address_3", "Doesn't exist")
            )
            .withColumn(
                "name",
                F.regexp_replace(F.col("name"), "Â", "")
            )
            .withColumn(
                "phone",
                utils.clean_phone("phone")
            )
            .withColumn(
                "postalcode",
                F.when(
                    F.col("postal_code").isNull(),
                    F.lit("Unknown")
                )
                .when(
                    F.col("country") == "United States",
                    F.regexp_replace(F.col("postal_code"), "-.*", "")
                )
                .otherwise(
                    F.col("postal_code")
                )
            )
            .withColumn(
                "ingestion_ts", 
                F.lit(run_ts) #da utlizzare come sequence_by
            )
            .withColumn(
                "brewery_sk",
                F.sha2(
                    F.concat_ws(
                        "_",
                        F.col("id"), #chiave naturale
                        F.col("ingestion_ts").cast("string")
                        ), 256
                    )
            )
            .select( 
                'brewery_sk',           
                'id',
                'name', 
                'brewery_type',
                'address_1', 
                'address_2', 
                'address_3', 
                'street',
                'city',
                'country',
                'postal_code', 
                'state', 
                'state_province',  
                'latitude', 
                'longitude', 
                'phone',      
                'website_url', 
                'ingestion_ts'
            )
        
        )
    
    return df
