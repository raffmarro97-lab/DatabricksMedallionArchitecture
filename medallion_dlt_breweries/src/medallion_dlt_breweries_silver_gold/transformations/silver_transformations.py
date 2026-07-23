from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from medallion_dlt_breweries_silver_gold.utilities import utils

def transform_silver_breweries(df: DataFrame) -> DataFrame:
    return (
        
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
            'website_url',
            'ingestion_ts'
        )
        .where(
            (F.col("country") == "United States") &
            (F.col("address_1").isNotNull())
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
            "postal_code",
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
            "brewery_sk",
            F.sha2(
                F.concat_ws(
                    "_",
                    F.col("id"),  # chiave naturale
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