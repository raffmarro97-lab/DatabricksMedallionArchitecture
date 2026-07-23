from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from medallion_dlt_breweries_silver_gold.utilities.utils import (
    clean_phone,
    fill_null,
)


def test_clean_phone(spark):
    schema = StructType(
        [
            StructField("row_id", IntegerType(), False),
            StructField("phone", StringType(), True),
        ]
    )

    input_df = spark.createDataFrame(
        [
            (1, "+39 333-123-4567"),
            (2, "(555) 123-4567"),
            (3, "333 555 7777"),
            (4, None),
        ],
        schema=schema,
    )

    rows = (
        input_df
        .withColumn("clean_phone", clean_phone("phone"))
        .orderBy("row_id")
        .collect()
    )

    actual = {
        row["row_id"]: row["clean_phone"]
        for row in rows
    }

    assert actual == {
        1: "3331234567",
        2: "5551234567",
        3: "3335557777",
        4: "Unknown",
    }


def test_fill_null(spark):
    schema = StructType(
        [
            StructField("row_id", IntegerType(), False),
            StructField("address_2", StringType(), True),
        ]
    )

    input_df = spark.createDataFrame(
        [
            (1, "Existing address"),
            (2, None),
        ],
        schema=schema,
    )

    rows = (
        input_df
        .withColumn(
            "filled_address",
            fill_null("address_2", "Doesn't exist"),
        )
        .orderBy("row_id")
        .collect()
    )

    actual = {
        row["row_id"]: row["filled_address"]
        for row in rows
    }

    assert actual == {
        1: "Existing address",
        2: "Doesn't exist",
    }