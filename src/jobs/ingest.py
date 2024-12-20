import os
from pyspark.sql import SparkSession

from schemas import flights_schema


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


# filename = '/home/iceberg/data/flights.csv'
flights_filename = '../../data/flights.csv'
airlines_filename = '../../data/airlines.csv'
airports_filename = '../../data/airports.csv'
cancellation_codes_filename = '../../data/cancellation_codes.csv'

# Read .csv files


# Write to Bronze (Raw) Iceberg tables

