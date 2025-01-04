from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from chispa import assert_df_equality
from ..jobs.load import create_iceberg_tables, write_audit_publish_iceberg, write_iceberg, deduplicate_input_all_columns_against_source


def test_create_iceberg_tables(spark):
    pass


def test_write_audit_publish_iceberg(spark):
    pass


def test_write_iceberg(spark):
    pass


def test_deduplicate_input_all_columns_against_source(spark):
    # Define schema and input test data
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])
    input_data = [("Alice", 30), ("Alice", 30), ("Bob", 25), ("Charlie", 35)]
    source_data = [("Alice", 30), ("Bob", 25)]

    # Create input and source DataFrames    
    input_df = spark.createDataFrame(input_data, schema)
    source_df = spark.createDataFrame(source_data, schema)
    
    # Deduplicate input data across all columns from source df
    result_df = deduplicate_input_all_columns_against_source(input_df, source_df)
    
    # Define expected data and DataFrame
    expected_data = [("Charlie", 35)]
    expected_df = spark.createDataFrame(expected_data, schema)
    
    assert_df_equality(result_df, expected_df)