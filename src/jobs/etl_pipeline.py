import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

import iceberg_ddl
from extract import extract_raw_data, generate_dim_dates_df
from transform import do_raw_flights_transformation, do_agg_fact_flights_transformation
from load import create_iceberg_tables, write_audit_publish_iceberg, write_iceberg


CATALOG_NAME = os.environ['CATALOG_NAME']
DATABASE_NAME = os.environ['DATABASE_NAME']


def init_spark() -> SparkSession:
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
    
    return spark


def main():
    # Initialize spark
    spark = init_spark()

    ## Extract raw data
    flights_df, airlines_df, airports_df, cancel_codes_df = extract_raw_data(spark)
    
    # Create dim_dates df
    dates_df = generate_dim_dates_df(spark)

    ## Transform flights data (Create "date" column and add "is_delayed" column)
    flights_df = do_raw_flights_transformation(spark, flights_df)

    ## Load source data into Iceberg (Silver-level)
    # Create Iceberg tables if not exists
    create_iceberg_tables(spark)

    # Write-Audit-Publish fact_flights to Iceberg
    write_audit_publish_iceberg(spark, flights_df, 'fact_flights', iceberg_ddl.update_flights_ddl)

    # Write dimension tables to Iceberg
    write_iceberg(spark, airlines_df, 'dim_airlines')
    write_iceberg(spark, airports_df, 'dim_airports')
    write_iceberg(spark, cancel_codes_df, 'dim_cancel_codes')
    write_iceberg(spark, dates_df, 'dim_dates')


    ## Load aggregated fact table into Iceberg (Gold-level)
    # Perform aggregated fact table transformation
    
    # Write-Audit-Publish aggregated fact table to Iceberg



if __name__ == "__main__":
    main()
