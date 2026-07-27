from datetime import datetime

from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from medallion_dlt_breweries_silver_gold.transformations.silver_transformations import (
    transform_silver_breweries,
)


def test_transform_silver_breweries(spark):
    schema = StructType(
        [
            StructField("address_1", StringType(), True),
            StructField("address_2", StringType(), True),
            StructField("address_3", StringType(), True),
            StructField("brewery_type", StringType(), True),
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("id", StringType(), False),
            StructField("latitude", StringType(), True),
            StructField("longitude", StringType(), True),
            StructField("name", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("postal_code", StringType(), True),
            StructField("state", StringType(), True),
            StructField("state_province", StringType(), True),
            StructField("street", StringType(), True),
            StructField("website_url", StringType(), True),
            StructField("ingestion_ts", TimestampType(), False),
        ]
    )

    input_data = [
        (
            "100 Main Street",
            None,
            None,
            "micro",
            "Denver",
            "United States",
            "brewery-1",
            "39.7392",
            "-104.9903",
            "Brewery ÂOne",
            "(555) 123-4567",
            "80202-1234",
            "Colorado",
            "Colorado",
            "100 Main Street",
            None,
            datetime(2026, 7, 23, 10, 0, 0),
        ),
    ]

    input_df = spark.createDataFrame(
        input_data,
        schema=schema,
    )

    rows = transform_silver_breweries(input_df).collect()

    assert len(rows) == 1

    row = rows[0]

    assert row["name"] == "Brewery One"
    assert row["phone"] == "5551234567"
    assert row["postal_code"] == "80202"
    assert row["address_2"] == "Doesn't exist"
    assert row["address_3"] == "Doesn't exist"
    assert row["brewery_sk"] is not None