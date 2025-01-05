import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import expr

import iceberg_ddl
from data_quality import run_data_quality_checks

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


# TODO how to handle cleaning up audit branches? Auto cleanup, ie. expire branch?
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
        True if write and publish is successful, False otherwise
    """
    published = False
    
    # Filter new records and check for updates, return if no new records
    input_df, has_new_records = filter_new_records_and_check_updates(spark, input_df, table_name)
    if not has_new_records:
        return published
    
    # Create audit branch
    audit_branch_name = f'audit_{table_name}'
    spark.sql(f'ALTER TABLE {CATALOG_NAME}.{DATABASE_NAME}.{table_name} CREATE BRANCH {audit_branch_name}')
    
    # Set "write.wap.enabled" table property
    spark.sql(f'ALTER TABLE {CATALOG_NAME}.{DATABASE_NAME}.{table_name} SET TBLPROPERTIES ("write.wap.enabled" = "true")')
    
    # Set "spark.wap.branch" to ensure we write to audit branch
    spark.conf.set('spark.wap.branch', audit_branch_name)
    
    ## Write to audit branch
    # Create temp view to use during write
    input_view = f'input_df'
    input_df.createOrReplaceTempView(input_view)
    
    # Write data to table using merge/upsert
    merge_ddl = merge_ddl.format(
        CATALOG_NAME=CATALOG_NAME,
        DATABASE_NAME=DATABASE_NAME,
        input_view_name=input_view
    )
    spark.sql(merge_ddl)
    
    ## Perform Data Quality checks
    # Load data from newly staged table
    staged_df = spark.table(f'{CATALOG_NAME}.{DATABASE_NAME}.{table_name}')
    
    # Run Data Quality checks
    result = run_data_quality_checks(spark, staged_df, table_name)

    # Publish if DQ passes, else drop branch and raise error
    if result.status == 'Success':
        # Publish audit branch to main branch
        print('Data Quality checks passed! Publishing audit branch...')
        spark.sql(f'CALL {CATALOG_NAME}.system.fast_forward("{DATABASE_NAME}.{table_name}", "main", "{audit_branch_name}")')
        published = True
        
    else:
        # Drop branch if DQ fails
        print(f'Data Quality checks failed. Dropping audit branch: {audit_branch_name}')
        spark.sql(f'ALTER TABLE {CATALOG_NAME}.{DATABASE_NAME}.{table_name} DROP BRANCH `{audit_branch_name}`')
        # Raise error
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
    written = False
    
    # Filter new records and check for updates, return if no new records
    input_df, has_new_records = filter_new_records_and_check_updates(spark, input_df, table_name)
    if not has_new_records:
        return written
    
    try:
        if mode == 'append':
            input_df.writeTo(f'{CATALOG_NAME}.{DATABASE_NAME}.{table_name}').append()
            written = True
        return written
    
    except Exception as e:
        # Log the exception if needed
        print(f"Error writing to Iceberg table: {e}")
        return written


def remove_existing_records_from_input(
    input_df: DataFrame, 
    source_df: DataFrame
) -> DataFrame:
    """
    Remove duplicates from input DataFrame by comparing against the source DataFrame by performing a null-safe anti-join.
    By default, spark does not handle null comparisons well in joins, so we need to generate a null-safe join condition for all columns.
    
    Args:
        input_df: Input DataFrame
        source_df: Source DataFrame to check for duplicates
    
    Returns:
        DataFrame with duplicates removed
    """
    # Generate null-safe join condition for all columns
    join_condition = " AND ".join([f"input_df.{col} <=> source_df.{col}" for col in input_df.columns])

    # Perform left anti-join to remove duplicates
    result_df = input_df.alias('input_df').join(
        source_df.alias('source_df'),
        on=expr(join_condition),
        how='left_anti'
    )
    
    return result_df


def filter_new_records_and_check_updates(
    spark: SparkSession,
    input_df: DataFrame,
    table_name: str
) -> tuple[DataFrame, bool]:
    """
    Deduplicate input DataFrame against the source table and check if there are new records to write.
    
    Args:
        spark: SparkSession object
        input_df: Input DataFrame
        table_name: Name of the Iceberg table
    
    Returns:
        Tuple containing the deduplicated DataFrame and a flag indicating if there are new records to write
    """
    # Dedup input data across all columns from source table
    source_df = spark.table(f'{CATALOG_NAME}.{DATABASE_NAME}.{table_name}')
    deduped_df = remove_existing_records_from_input(input_df, source_df)
    
    # Check if there are new records to write
    has_new_records = deduped_df.count() > 0
    if not has_new_records:
        print(f" * No new records to write to {table_name}")
    
    return deduped_df, has_new_records