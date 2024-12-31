import tempfile
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from chispa import assert_df_equality

from ..jobs.extract import read_csv, generate_dim_date_df


def test_read_csv(spark):
    # Define schema and input test data
    data = [("Alice", 30), ("Bob", 25)]
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])
    
    # Create input df
    df = spark.createDataFrame(data, schema)
    
    # Write input df to a temporary .csv file, and read it back
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_csv_path = f'{temp_dir}/test.csv'
        
        # Write input df to .csv
        df.write.csv(temp_csv_path, header=True)
        
        # Read .csv file
        result_df = read_csv(spark, temp_csv_path, schema)
        
        # Assert that the result df has the same schema and data as the input df
        assert_df_equality(df, result_df)


def test_generate_dim_date_df(spark):
    df = generate_dim_date_df(spark)
    
    # Ensure row count is correct
    assert df.count() == 365
    
    # Ensure that the schema is correct
    expected_columns = ['date', 'year', 'month', 'day', 'day_of_week', 'quarter', 'holiday_name', 'is_holiday']
    assert df.columns == expected_columns, "Schema does not match expected schema."
    
    # Ensure that non-holidays have null holiday_name
    non_holidays_df = df.filter(df.is_holiday == lit(False))
    assert non_holidays_df.filter(non_holidays_df.holiday_name.isNotNull()).count() == 0, "Non-holiday rows should not have a holiday name."
    
    # Ensure that the date range is correct
    min_date = df.selectExpr("MIN(date) as min_date").collect()[0]['min_date']
    max_date = df.selectExpr("MAX(date) as max_date").collect()[0]['max_date']
    assert str(min_date) == "2015-01-01", "Minimum date is incorrect."
    assert str(max_date) == "2015-12-31", "Maximum date is incorrect."