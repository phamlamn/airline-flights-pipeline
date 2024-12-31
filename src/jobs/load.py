import os
from pyspark.sql import SparkSession, DataFrame


CATALOG_NAME = os.environ['CATALOG_NAME']
DATABASE_NAME = os.environ['DATABASE_NAME']


def write_audit_publish(
    spark: SparkSession,
    input_df: DataFrame,
    table_name: str,
) -> bool:
    
    # Create audit branch, and set spark.wap.branch to ensure we write to audit branch
    audit_branch_name = f'audit_{table_name}'
    spark.sql(f'ALTER TABLE {CATALOG_NAME}.{DATABASE_NAME}.{table_name} CREATE BRANCH {audit_branch_name}')
    spark.conf.set('spark.wap.branch', audit_branch_name)
    
    ## Write to audit branch
    # TODO Dedup input data?
    
    # Create temp view to use during write
    input_view_name = f'{table_name}_source'
    input_df.createOrReplaceTempView(input_view_name)
    
    # Write flights data to table
    flights_merge_ddl = f"""
    MERGE INTO airline.db.flights t
    USING {input_view_name} s
        ON  t.date = s.date
        AND t.airline = s.airline
        AND t.flight_number = s.flight_number
        AND t.scheduled_departure = s.scheduled_departure
    WHEN NOT MATCHED THEN INSERT *
    """
    spark.sql(flights_merge_ddl)
    
    # Create df from newly staged table
    staged_df = spark.table(f'{CATALOG_NAME}.{DATABASE_NAME}.{table_name}')
    
    # Perform Data Quality checks
    result = run_data_quality_checks(spark, staged_df, table_name)

    # Publish if DQ passes, else log failure and break
    # TODO fastforward or cherrypick?
    if result.status == 'Success':
        spark.sql(f'CALL airline.system.fast_forward("db.flights", "main", "{audit_branch_name}")')
        # # Get snapshot id
        # spark.sql('SELECT snapshot_id FROM airline.db.flights.refs WHERE name="audit_branch_flights"').show()
        # spark.sql('CALL airline.system.cherrypick_snapshot("db.flights", 668148544964107792)')
    else:
        raise ValueError('Data Quality checks failed.')
        # raise ValueError('Data Quality checks failed. Publishing aborted.')
    
    return

def write(
    spark: SparkSession,
    df: DataFrame,
    table_name: str
) -> None:
    df.writeTo(f'{CATALOG_NAME}.{DATABASE_NAME}.{table_name}').append()
    return