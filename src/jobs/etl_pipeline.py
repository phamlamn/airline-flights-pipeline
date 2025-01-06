import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

import iceberg_ddl
from extract import extract_raw_data, generate_dim_dates_df
from transform import remove_duplicate_records, do_raw_flights_transformation, do_agg_fact_flights_transformation
from load import create_iceberg_tables, write_audit_publish_iceberg, write_iceberg


CATALOG_NAME = os.environ['CATALOG_NAME']
DATABASE_NAME = os.environ['DATABASE_NAME']


def init_spark(app_name: str = 'US Flights Pipeline') -> SparkSession:
    # Define the configuration for SparkSession, including the Iceberg catalog
    # which uses MinIO (an S3-compatible object storage) for local storage.
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
        .appName(app_name) \
        .config(map=spark_configs) \
        .getOrCreate()
    
    return spark


def main():
    # Initialize spark
    spark = init_spark()

    # ==============================================================================
    ## Extract raw data
    print("Extracting raw data...")
    
    flights_df, airlines_df, airports_df, cancel_codes_df = extract_raw_data(spark)
    
    # Create dim_dates df
    dates_df = generate_dim_dates_df(spark)

    # ==============================================================================
    ## Transform flights data
    print("Transforming raw data...")
    
    # Perform raw flights transformation (Create "date" column and add "is_delayed" column)
    flights_df = do_raw_flights_transformation(spark, flights_df)
    
    # Remove duplicate flights
    flights_df = remove_duplicate_records(spark, flights_df, ["date", "airline", "flight_number", "scheduled_departure"])

    # ==============================================================================
    ## Load source data into Iceberg (Silver-level)
    print("Loading source data into Iceberg...")
    
    # Create Iceberg tables if not exists
    create_iceberg_tables(spark)

    # Write-Audit-Publish fact_flights to Iceberg (Idempotent)
    print("Writing (WAP) fact_flights to Iceberg...")
    result = write_audit_publish_iceberg(spark, flights_df, 'fact_flights', iceberg_ddl.merge_fact_flights_ddl)
    print(f"Write-Audit-Publish result: {result}")

    # Write dimension tables to Iceberg (Idempotent)
    print("Writing dim tables to Iceberg...")
    write_iceberg(spark, airlines_df, 'dim_airlines')
    write_iceberg(spark, airports_df, 'dim_airports')
    write_iceberg(spark, cancel_codes_df, 'dim_cancellation_codes')
    write_iceberg(spark, dates_df, 'dim_dates')


    # ==============================================================================
    ## Load aggregated fact table into Iceberg (Gold-level)
    # Perform aggregated fact table transformation
    agg_df = do_agg_fact_flights_transformation(spark)
    
    # TODO implement Data Quality for agg_fact_flights
    # Write-Audit-Publish aggregated fact table to Iceberg
    result = write_audit_publish_iceberg(spark, agg_df, 'agg_fact_flights', iceberg_ddl.merge_agg_fact_flights_ddl)



if __name__ == "__main__":
    main()
