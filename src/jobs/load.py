import os
from pyspark.sql import SparkSession, DataFrame

import iceberg_ddl
from ..data_quality import run_data_quality_checks

CATALOG_NAME = os.environ['CATALOG_NAME']
DATABASE_NAME = os.environ['DATABASE_NAME']


def create_iceberg_tables(spark: SparkSession) -> None:
    """
    Create (if not exists) Iceberg tables for the US Flights dataset.
    
    Args:
        spark: SparkSession object
    """
    # Create Iceberg tables if not exists
    spark.sql(iceberg_ddl.fact_flights_ddl)
    spark.sql(iceberg_ddl.dim_airlines_ddl)
    spark.sql(iceberg_ddl.dim_airports_ddl)
    spark.sql(iceberg_ddl.dim_cancel_codes_ddl)
    spark.sql(iceberg_ddl.dim_dates_ddl)


def write_audit_publish_iceberg(
    spark: SparkSession,
    input_df: DataFrame,
    table_name: str,
    merge_ddl: str
) -> bool:
    """
    Write DataFrame to Iceberg table branch, perform data quality checks, and publish to main branch if passed data quality checks.
    
    Args:
        spark: SparkSession object
        input_df: Input DataFrame
        table_name: Name of the Iceberg table
    
    Returns:
        True if write and publish is successful
    """
    
    # Create audit branch, and set "spark.wap.branch" to ensure we write to audit branch
    audit_branch_name = f'audit_{table_name}'
    spark.sql(f'ALTER TABLE {CATALOG_NAME}.{DATABASE_NAME}.{table_name} CREATE BRANCH {audit_branch_name}')
    spark.conf.set('spark.wap.branch', audit_branch_name)
    
    # Dedup input data across all columns from source table
    input_df = deduplicate_across_all_columns(
        input_df,
        spark.table(f'{CATALOG_NAME}.{DATABASE_NAME}.{table_name}')
    )
    
    ## Write to audit branch
    # Create temp view to use during write
    input_df.createOrReplaceTempView(table_name)
    
    # Write data to table using upsert/merge
    merge_ddl = merge_ddl.format(input_view_name=table_name)
    spark.sql(merge_ddl)
    
    ## Perform Data Quality checks
    # Create df from newly staged table
    staged_df = spark.table(f'{CATALOG_NAME}.{DATABASE_NAME}.{table_name}')
    
    # Run Data Quality checks
    result = run_data_quality_checks(spark, staged_df, table_name)

    # Publish if DQ passes, else log failure and break
    published = False
    if result.status == 'Success':
        # TODO fastforward or cherrypick?
        spark.sql(f'CALL airline.system.fast_forward("db.flights", "main", "{audit_branch_name}")')
        published = True
        # # Get snapshot id
        # spark.sql('SELECT snapshot_id FROM airline.db.flights.refs WHERE name="audit_branch_flights"').show()
        # spark.sql('CALL airline.system.cherrypick_snapshot("db.flights", 668148544964107792)')
    else:
        raise ValueError('Data Quality checks failed. Publishing aborted.')
    
    return published


def write_iceberg(
    spark: SparkSession,
    input_df: DataFrame,
    table_name: str,
    mode: str = 'append'
) -> bool:
    """
    Write DataFrame to Iceberg table.
    
    Args:
        spark: SparkSession object
        df: Input DataFrame
        table_name: Name of the Iceberg table
        mode: Write mode (append or overwrite)
        
    Returns:
        True if write is successful, False otherwise
    """
    # Dedup input data across all columns from source table
    input_df = deduplicate_across_all_columns(
        input_df,
        spark.table(f'{CATALOG_NAME}.{DATABASE_NAME}.{table_name}')
    )
    
    try:
        if mode == 'append':
            input_df.writeTo(f'{CATALOG_NAME}.{DATABASE_NAME}.{table_name}').append()
        return True
    
    except Exception as e:
        # Log the exception if needed
        print(f"Error writing to Iceberg table: {e}")
        return False


def deduplicate_across_all_columns(
    input_df: DataFrame, 
    source_df: DataFrame
) -> DataFrame:
    """
    Deduplicate input DataFrame across all columns by performing an anti-join with the source DataFrame.
    
    Args:
        input_df: Input DataFrame
        source_df: Source DataFrame to check for duplicates
    
    Returns:
        DataFrame with duplicates removed
    """
    # Perform anti-join to remove duplicates
    result_df = input_df.join(
        source_df,
        on=input_df.columns,
        how='left_anti'
    )
    
    return result_df