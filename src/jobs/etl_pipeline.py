import os
from pyspark.sql import SparkSession, DataFrame
# from pyspark.sql.functions import col, lit, expr, when, concat_ws, to_date, year, month, day, dayofweek, quarter
from pyspark.sql.types import StructType

import raw_schemas
import iceberg_ddl
from data_quality_definitions import *
from extract import read_csv, generate_dim_date_df
from transform import do_raw_flights_transformation, do_agg_fact_flights_transformation
from load import write_audit_publish


CATALOG_NAME = os.environ['CATALOG_NAME']
DATABASE_NAME = os.environ['DATABASE_NAME']


def main():
    # Define config for SparkSession, such as the Iceberg catalog
    # which utilizes minio (S3-compatible) for local object storage,
    spark_configs = {
        'spark.master': 'spark://spark-iceberg:7077',
        f'spark.sql.catalog.{CATALOG_NAME}': 'org.apache.iceberg.spark.SparkCatalog',
        f'spark.sql.catalog.{CATALOG_NAME}.type': 'rest',
        f'spark.sql.catalog.{CATALOG_NAME}.uri': 'http://rest:8181',
        f'spark.sql.catalog.{CATALOG_NAME}.s3.endpoint': 'http://minio:9000',
        f'spark.sql.catalog.{CATALOG_NAME}.warehouse': 's3://warehouse',
        f'spark.sql.catalog.{CATALOG_NAME}.io-impl': 'org.apache.iceberg.aws.s3.S3FileIO',
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
    cancel_codes_filename = f'{data_path}/cancellation_codes.csv'

    # Extract raw .csv files to DataFrame
    flights_df = read_csv(spark, flights_filename, raw_schemas.flights_schema)
    airlines_df = read_csv(spark, airlines_filename, raw_schemas.airlines_schema)
    airports_df = read_csv(spark, airports_filename, raw_schemas.airports_schema)
    cancel_codes_df = read_csv(spark, cancel_codes_filename, raw_schemas.cancel_codes_schema)

    # Transform flights data (Create "date" column and add "is_delayed" column)
    flights_df = do_raw_flights_transformation(spark, flights_df)

    # Create dim_date df
    date_df = generate_dim_date_df(spark)


    ## Load source data into Iceberg (Silver-level)
    # Create Iceberg tables if not exists
    spark.sql(iceberg_ddl.flights_ddl)
    spark.sql(iceberg_ddl.airlines_ddl)
    spark.sql(iceberg_ddl.airports_ddl)
    spark.sql(iceberg_ddl.cancel_codes_ddl)

    # Write-Audit-Publish flights to Iceberg
    write_audit_publish(spark, flights_df, 'fact_flights')


    ## Load aggregated fact table into Iceberg (Gold-level)
    # Perform aggregated fact table transformation
    
    # Write-Audit-Publish aggregated fact table to Iceberg



if __name__ == "__main__":
    main()
