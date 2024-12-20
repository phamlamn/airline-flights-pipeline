import schemas
import DDL

import os

CATALOG_NAME = os.environ['CATALOG_NAME']
DATABASE_NAME = os.environ['DATABASE_NAME']

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

