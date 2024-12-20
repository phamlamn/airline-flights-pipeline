
import os
from pyspark.sql import SparkSession

import schemas
import DDL

CATALOG_NAME = os.environ['CATALOG_NAME']

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


# Read .csv files
flights_df = spark.read \
    .schema(schemas.flights_schema) \
    .option('header', True) \
    .csv(flights_filename)
    
airlines_df = spark.read \
    .schema(schemas.airlines_schema) \
    .option('header', True) \
    .csv(airlines_filename)

airports_df = spark.read \
    .schema(schemas.airports_schema) \
    .option('header', True) \
    .csv(airports_filename)

cancellation_codes_df = spark.read \
    .schema(schemas.cancellation_codes_schema) \
    .option('header', True) \
    .csv(cancellation_codes_filename)


# Create Iceberg tables
spark.sql(DDL.flights_ddl)
spark.sql(DDL.airlines_ddl)
spark.sql(DDL.airports_ddl)
spark.sql(DDL.cancellation_codes_ddl)


# Write to Iceberg Tables
flights_df.writeTo(f'{CATALOG_NAME}.{DATABASE_NAME}.flights') \
    .append()

airlines_df.writeTo(f'{CATALOG_NAME}.{DATABASE_NAME}.airlines') \
    .append()

airports_df.writeTo(f'{CATALOG_NAME}.{DATABASE_NAME}.airports') \
    .append()

cancellation_codes_df.writeTo(f'{CATALOG_NAME}.{DATABASE_NAME}.cancellation_codes') \
    .append()

