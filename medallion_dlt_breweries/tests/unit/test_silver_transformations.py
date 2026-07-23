from datetime import datetime

from medallion_dlt_breweries_silver_gold.silver_transformations import(
    transform_silver_breweries,
)

def test_transform_silver_breweries(spark):
    input_df = spark.createDataFrame(
        input_df = spark.createDataFrame(
        [
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
                "https://example.com",
                datetime(2026, 7, 23, 10, 0, 0),
            ),
            (
                None,
                None,
                None,
                "micro",
                "Austin",
                "United States",
                "brewery-2",
                "30.2672",
                "-97.7431",
                "Filtered Brewery",
                "5550000000",
                "78701",
                "Texas",
                "Texas",
                None,
                None,
                datetime(2026, 7, 23, 10, 0, 0),
            ),
            (
                "1 Canadian Street",
                None,
                None,
                "micro",
                "Toronto",
                "Canada",
                "brewery-3",
                "43.6532",
                "-79.3832",
                "Canadian Brewery",
                "5551111111",
                "M5V 3A8",
                "Ontario",
                "Ontario",
                "1 Canadian Street",
                None,
                datetime(2026, 7, 23, 10, 0, 0),
            ),
        ],
        [
            "address_1",
            "address_2",
            "address_3",
            "brewery_type",
            "city",
            "country",
            "id",
            "latitude",
            "longitude",
            "name",
            "phone",
            "postal_code",
            "state",
            "state_province",
            "street",
            "website_url",
            "ingestion_ts",
        ],
        )
    )

    result = transform_silver_breweries(input_df).collect()

    assert len(result) == 1

    row = result[0]

    assert row["id"] == "brewery-1"
    assert row["address_2"] == "Doesn't exist"
    assert row["address_3"] == "Doesn't exist"
    assert row["name"] == "Brewery name"
    assert row["postal_code"] == "80202"
    assert row["brewery_sk"] is not None