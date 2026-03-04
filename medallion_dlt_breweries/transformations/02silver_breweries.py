from pyspark import pipelines as dp
from pyspark.sql.functions import col, count, count_if
from pyspark.sql import functions as F
from utilities import utils

# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.

@dp.table(
    name = "silver_brewries",
    comment = "Cleaning the bronze table and add ingestion_ts"
)
def silver_breweries():
    df = spark.read.table("bronze_brewries")
    run_ts = spark.sql("SELECT current_timestamp()").collect()[0][0]
    df =
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
            utils.clean_phone(F.col("phone") )
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
            F.lit(run_ts) #da utlizzare coem sequence_by
        )

    return df
