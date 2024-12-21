import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, expr, when, concat_ws, to_date, year, month, day, dayofweek, quarter

import schemas
import DDL


CATALOG_NAME = os.environ['CATALOG_NAME']
DATABASE_NAME = os.environ['DATABASE_NAME']


def read_csv(spark: SparkSession, filename: String, schema: StructType) -> DataFrame:
    return spark.read \
                .schema(schema) \
                .option('header', True) \
                .csv(filename)


def write_to_iceberg_table(
    spark: SparkSession,
    input_df: DataFrame,
    table_name: String
) -> None:
    deduped_df = input_df.dropDuplictes([])
    
    # deduplicated_df = input_df.dropDuplicates([primary_key])

    # if mode == "overwrite":
    #     deduplicated_df.writeTo(table_name) \
    #         .option("overwrite-mode", "dynamic") \
    #         .overwritePartitions()
    # elif mode == "append":
    #     deduplicated_df.writeTo(table_name).append()
    # elif mode == "merge":
    #     deduplicated_df.createOrReplaceTempView("incoming_data")
    #     spark.sql(f"""
    #     MERGE INTO {table_name} AS target
    #     USING incoming_data AS source
    #     ON target.{primary_key} = source.{primary_key}
    #     WHEN MATCHED THEN UPDATE SET *
    #     WHEN NOT MATCHED THEN INSERT *
    #     """)
    # else:
    #     raise ValueError(f"Unsupported mode: {mode}")
    pass


# TODO Generalize to any year
def generate_dim_date_df(spark: SparkSession) -> DataFrame:
    # Initialize date_df
    date_df = spark.range(365) \
        .withColumn('date', expr('date_add("2015-01-01", CAST(id AS INT))')) \
        .withColumn('year', year('date')) \
        .withColumn('month', month('date')) \
        .withColumn('day', day('date')) \
        .withColumn('day_of_week', dayofweek('date')) \
        .withColumn('quarter', quarter('date')) \
        .drop('id')

    # List of U.S. federal holidays
    us_holidays_2015 = [
        ("2015-01-01", "New Year's Day"),
        ("2015-01-19", "Martin Luther King Jr. Day"),
        ("2015-02-16", "Presidents' Day"),
        ("2015-05-25", "Memorial Day"),
        ("2015-07-04", "Independence Day"),
        ("2015-09-07", "Labor Day"),
        ("2015-10-12", "Columbus Day"),
        ("2015-11-11", "Veterans Day"),
        ("2015-11-26", "Thanksgiving Day"),
        ("2015-12-25", "Christmas Day"),
    ]
    
    # Create holidays_df and cast date from STRING to DATE type
    holidays_df = spark.createDataFrame(us_holidays_2015, ['holiday_date', 'holiday_name'])
    holidays_df = holidays_df.withColumn('holiday_date', to_date('holiday_date'))

    # Join holidays to date_df and add is_holiday column
    date_df = date_df \
        .join(
            holidays_df,
            date_df.date == holidays_df.holiday_date,
            'left'
        ) \
        .withColumn(
            'is_holiday',
            when(col('holiday_name').isNotNull(), lit(True)).otherwise(lit(False))
        ) \
        .drop('holiday_date') \
        .sort('date')

    # Rearrange date to be first column,
    date_df = date_df.select('date', *[col(c) for c in date_df.columns if c != 'date'])
    return date_df


def main():
    # Define config for SparkSession, such as the Iceberg catalog
    # which utilizes minio (S3-compatible) for local object storage,
    spark_configs = {
        'spark.master': 'spark://spark-iceberg:7077',
        f'spark.sql.catalog.{CATALOG_NAME}': 'org.apache.iceberg.spark.SparkCatalog',
        f'spark.sql.catalog.{CATALOG_NAME}.io-impl': 'org.apache.iceberg.aws.s3.S3FileIO',
        f'spark.sql.catalog.{CATALOG_NAME}.s3.endpoint': 'http://minio:9000',
        f'spark.sql.catalog.{CATALOG_NAME}.type': 'rest',
        f'spark.sql.catalog.{CATALOG_NAME}.uri': 'http://rest:8181',
        f'spark.sql.catalog.{CATALOG_NAME}.warehouse': 's3://warehouse',
        'spark.sql.defaultCatalog': CATALOG_NAME
    }

    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName('US Flights Pipeline') \
        .config(map=spark_configs) \
        .getOrCreate()

    # Define .csv filepaths
    data_path = '/home/iceberg/data'
    flights_filename = f'{data_path}/flights.csv'
    airlines_filename = f'{data_path}/airlines.csv'
    airports_filename = f'{data_path}/airports.csv'
    cancellation_codes_filename = f'{data_path}/cancellation_codes.csv'

    # Read .csv files to DataFrame
    flights_df = read_csv(spark, flights_filename, schemas.flights_schema)
    airlines_df = read_csv(spark, airlines_filename, schemas.airlines_schema)
    airports_df = read_csv(spark, airports_filename, schemas.airports_schema)
    cancellation_codes_df = read_csv(spark, cancellation_codes_filename, schemas.cancellation_codes_schema)
    
    ## Create dim_date df
    date_df = generate_dim_date_df(spark)
    
    # Add date column to flights_df and drop year, month, day, day_of_week columns
    flights_df = flights_df \
        .withColumn('date', to_date(concat_ws('-', 'year', 'month', 'day'))) \
        .drop('year', 'month', 'day', 'day_of_week')
        

    # Create Iceberg tables
    spark.sql(DDL.flights_ddl)
    spark.sql(DDL.airlines_ddl)
    spark.sql(DDL.airports_ddl)
    spark.sql(DDL.cancellation_codes_ddl)

    # Write to Iceberg Tables
    # TODO Make idempotent, use upsert/merge?
    # TODO implement WAP for fact_flights table
    # TODO Land into bronze, transform flight to datetime, create date dimension table (alternatively, just add new column datetime)
    flights_df.writeTo(f'{CATALOG_NAME}.{DATABASE_NAME}.flights') \
        .append()

    airlines_df.writeTo(f'{CATALOG_NAME}.{DATABASE_NAME}.airlines') \
        .append()

    airports_df.writeTo(f'{CATALOG_NAME}.{DATABASE_NAME}.airports') \
        .append()

    cancellation_codes_df.writeTo(f'{CATALOG_NAME}.{DATABASE_NAME}.cancellation_codes') \
        .append()


if __name__ == "__main__":
    main()