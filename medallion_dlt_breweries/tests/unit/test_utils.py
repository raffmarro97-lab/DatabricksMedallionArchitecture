from medallion_dlt_breweries_silver_gold.utilities.utils import(
    clean_phone,
    fill_null
)
def test_clean_phone(spark):
    input_df = spark.createDataFrame(
        [
            (1, "+39 333-123-4567"),
            (2, "(555) 123-4567"),
            (3, "333 555 7777"),
            (4, None),
        ],
        ["row_id", "phone"],
    )

    result = (
        input_df
        .withColumn("clean_phone", clean_phone("phone"))
        .collect()
    )

    actual = {
        row["row_id"]: row["clean_phone"]
        for row in result
    }

    assert actual == {
        1: "3331234567",
        2: "5551234567",
        3: "3335557777",
        4: "Unknown",
    }

def test_fill_null(spark):
    input_df = spark.createDataFrame(
        [
            ("Existing address"),
            (None)
        ],
        ["address_2"]
    )

    result = (
        input_df
        .withColumn(
            "filled_address",
            fill_null("address_2", "Doesn't exist")
        )
        .collect()
    )

    actual = [row["filled_address"] for row in result]

    assert actual == [
        "Existing address",
        "Doesn't exist",
    ]