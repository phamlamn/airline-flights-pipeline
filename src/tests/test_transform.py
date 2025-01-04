from datetime import date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from chispa import assert_df_equality

from ..jobs.transform import remove_duplicate_flights, do_raw_flights_transformation, do_agg_fact_flights_transformation


def test_remove_duplicate_flights(spark):
    # Define schema and input test data
    schema = StructType([
        StructField("date", DateType(), True),
        StructField("airline", StringType(), True),
        StructField("flight_number", IntegerType(), True),
        StructField("scheduled_departure", IntegerType(), True),
        StructField("origin_airport", StringType(), True)
    ])
    input_data = [
        (date(2021, 1, 1), "AA", 100, 1000, "SFO"),
        (date(2021, 1, 1), "AA", 100, 1000, "PHL"),
        (date(2021, 1, 1), "AA", 101, 1000, "LAX"),
        (date(2021, 1, 2), "DL", 200, 1200, "JFK"),
        (date(2021, 1, 2), "DL", 200, 1200, "PHL"),
    ]
    # Create input DataFrame
    input_df = spark.createDataFrame(input_data, schema)
    
    # Deduplicate input data
    result_df = remove_duplicate_flights(spark, input_df)
    
    # Define expected output
    expected_data = [
        (date(2021, 1, 1), "AA", 100, 1000, "SFO"),
        (date(2021, 1, 1), "AA", 101, 1000, "LAX"),
        (date(2021, 1, 2), "DL", 200, 1200, "JFK"),
    ]
    # Create expected DataFrame
    expected_df = spark.createDataFrame(expected_data, schema)
    
    # Compare actual vs. expected DataFrame
    assert_df_equality(result_df, expected_df)


def test_do_raw_flights_transformation(spark):
    # Define schema and input test data
    pass


def test_do_agg_fact_flights_transformation(spark):
    # Define schema and input test data
    pass
