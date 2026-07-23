from medallion_dlt_breweries_silver_gold.transformations.gold_transformations import (
    aggregate_breweries,
)


def test_aggregate_breweries_by_state(spark):
    input_df = spark.createDataFrame(
        [
            ("1", "California"),
            ("2", "California"),
            ("3", "Texas"),
        ],
        ["id", "state"],
    )

    result = aggregate_breweries(input_df).collect()

    actual = {
        row["state"]: row["num_breweries"]
        for row in result
    }

    assert actual == {
        "California": 2,
        "Texas": 1,
    }