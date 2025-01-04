from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from chispa import assert_df_equality
from ..jobs.load import create_iceberg_tables, write_audit_publish_iceberg, write_iceberg, remove_existing_records_from_input, filter_new_records_and_check_updates


def test_create_iceberg_tables(spark):
    pass


def test_write_audit_publish_iceberg(spark):
    pass


def test_write_iceberg(spark):
    pass


def test_remove_existing_records_from_input(spark):
    # Define schema and input test data
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])
    input_data = [("Alice", 30), ("Alice", 30), ("Bob", 25), (None, 0), ("Charlie", 35)]
    source_data = [("Alice", 30), ("Bob", 25), (None, 0)]

    # Create input and source DataFrames    
    input_df = spark.createDataFrame(input_data, schema)
    source_df = spark.createDataFrame(source_data, schema)
    
    # Deduplicate input data across all columns from source df
    result_df = remove_existing_records_from_input(input_df, source_df)
    
    # Define expected data and DataFrame
    expected_data = [("Charlie", 35)]
    expected_df = spark.createDataFrame(expected_data, schema)
    
    assert_df_equality(result_df, expected_df)


# def test_filter_new_records_and_check_updates(spark):
#     # Define schema and input test data
#     schema = StructType([
#         StructField("id", IntegerType(), True),
#         StructField("name", StringType(), True),
#         StructField("age", IntegerType(), True)
#     ])
#     input_data = [(1, "Alice", 30), (2, "Bob", 25), (3, "Charlie", 35)]
#     source_data = [(1, "Alice", 30), (2, "Bob", 26)]

#     # Create input and source DataFrames    
#     input_df = spark.createDataFrame(input_data, schema)
#     source_df = spark.createDataFrame(source_data, schema)
    
#     # Filter new records and check updates
#     new_records_df, updated_records_df = filter_new_records_and_check_updates(input_df, source_df)
    
#     # Define expected data and DataFrames
#     expected_new_data = [(3, "Charlie", 35)]
#     expected_new_df = spark.createDataFrame(expected_new_data, schema)
    
#     expected_updated_data = [(2, "Bob", 25)]
#     expected_updated_df = spark.createDataFrame(expected_updated_data, schema)
    
#     assert_df_equality(new_records_df, expected_new_df)
#     assert_df_equality(updated_records_df, expected_updated_df)