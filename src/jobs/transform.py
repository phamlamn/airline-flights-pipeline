import os
from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.functions import to_date, concat_ws, when, col, lit, broadcast


CATALOG_NAME = os.environ['CATALOG_NAME']
DATABASE_NAME = os.environ['DATABASE_NAME']


def remove_duplicate_records(spark: SparkSession, input_df: DataFrame, columns: list[str]) -> DataFrame:
    """
    Remove duplicate flights based on unique columns in the input DataFrame.

    Args:
        spark: SparkSession object
        input_df: Input DataFrame
        columns: List of columns to deduplicate on
    
    Returns:
        DataFrame with duplicates removed
    """
    # Deduplicate over unique columns
    deduped_df = input_df.dropDuplicates(columns)
    return deduped_df


def do_raw_flights_transformation(spark: SparkSession, input_df: DataFrame) -> DataFrame:
    # Add date column and remove other date-related columns
    flights_df = input_df \
        .withColumn('date', to_date(concat_ws('-', 'year', 'month', 'day'))) \
        .drop('year', 'month', 'day', 'day_of_week')

    # Add is_delayed column (when scheduled_departure > 0)
    flights_df = flights_df \
        .withColumn(
            'is_delayed',
            when(col('departure_delay') > 0, lit(1)).otherwise(lit(0))
        )

    # Rearrange date to be first column,
    flights_df = flights_df.select('date', *[col(c) for c in flights_df.columns if c != 'date'])

    # Sort by date and schedule_departure time
    flights_df = flights_df.sort(['date', 'scheduled_departure'])
    
    return flights_df


def get_aggregation_level(columns: list[str]) -> Column:
    """
    Get the aggregation level based on the non-null columns in the input list.
    
    Args:
        columns: List of column names
    
    Returns:
        Column with the aggregation level (this gets computed when the DataFrame is executed)
    """
    # Concatenate non-nulls columns with '_' separator, eg. year_month_airline. (this produces a Column)
    agg_level = concat_ws(
        '_',
        *[when(col(c).isNotNull(), lit(c)) for c in columns]
    )
    
    # Iteratively build the condition for all_nulls
    # Check if all columns are null and set agg_level to 'all' if true
    all_nulls = lit(True)
    for c in columns:
        all_nulls &= col(c).isNull()
    agg_level = when(all_nulls, 'all').otherwise(agg_level)
    
    return agg_level


def agg_flight_metrics_by_grouping_sets(spark: SparkSession, view_name: str) -> DataFrame:
    """
    Perform aggregation by grouping sets on the input DataFrame.
    
    Args:
        spark: SparkSession
        view_name: View name for enriched_flights_df, assumes view to exist
    
    Returns:
        DataFrame with aggregated metrics
    """
    # Define aggregation query
    agg_query = f"""
    SELECT
        -- Grouping columns
        year,
        month,
        day_of_week,
        airline,
        origin_airport,

        -- Aggregation columns
        -- Total flights
        COUNT(*) AS total_flights,
        
        -- Delayed flights
        COUNT(CASE WHEN is_delayed = 1 THEN 1 END) AS delayed_flights,
        delayed_flights / total_flights AS delayed_rate,
        AVG(departure_delay) AS avg_delay_time,

        -- Cancelled flights
        COUNT(CASE WHEN cancelled = 1 THEN 1 END) AS cancelled_flights,
        cancelled_flights / total_flights AS cancelled_rate,

        -- Total cancellations for each reason
        COUNT(CASE WHEN cancellation_reason = 'A' THEN 1 END) AS cancellations_A,
        COUNT(CASE WHEN cancellation_reason = 'B' THEN 1 END) AS cancellations_B,
        COUNT(CASE WHEN cancellation_reason = 'C' THEN 1 END) AS cancellations_C,
        COUNT(CASE WHEN cancellation_reason = 'D' THEN 1 END) AS cancellations_D,

        -- Percentage of cancellations by each reason
        cancellations_A / cancelled_flights AS percent_cancellations_A,
        cancellations_B / cancelled_flights AS percent_cancellations_B,
        cancellations_C / cancelled_flights AS percent_cancellations_C,
        cancellations_D / cancelled_flights AS percent_cancellations_D
        
    FROM {view_name}

    -- Group by various levels of granularity using GROUPING SETS (equivalent to UNION ALL for multiple GROUP BY queries)
    GROUP BY GROUPING SETS (
        -- Group by time periods
        (year),
        (year, month),
        (day_of_week),
        
        -- Group by airline at different levels
        (airline), 
        (airline, year), 
        (airline, year, month),

        -- Group by origin airport at different levels
        (origin_airport), 
        (origin_airport, year), 
        (origin_airport, year, month)
    )
    """

    # Perform aggregation
    agg_df = spark.sql(agg_query)
    
    # Define aggregation level columns for time and non-time columns
    time_cols = ['year', 'month', 'day_of_week']
    non_time_cols = ['airline', 'origin_airport']
    
    # Get aggregation level for time and non-time columns
    time_agg_level = get_aggregation_level(time_cols)
    agg_level = get_aggregation_level(non_time_cols)
    
    # Add aggregation level columns to the DataFrame
    agg_df = agg_df \
        .withColumn('time_agg_level', time_agg_level) \
        .withColumn('agg_level', agg_level)

    return agg_df


def do_agg_fact_flights_transformation(spark: SparkSession):
    # Read fact_flights and dim_dates tables
    fact_flights_df = spark.read.table(f'{CATALOG_NAME}.{DATABASE_NAME}.fact_flights')
    dim_dates_df = spark.read.table(f'{CATALOG_NAME}.{DATABASE_NAME}.dim_dates')
        
    # Broadcast join fact_flights with dim_dates
    enriched_flights_df = fact_flights_df.join(
        broadcast(dim_dates_df),
        on=['date']
    )
    
    # Create temp view for fact_flights
    enriched_flights_view = 'enriched_flights_df'
    enriched_flights_df.createOrReplaceTempView(enriched_flights_view)
    
    # Aggregate flight metrics by grouping sets
    agg_df = agg_flight_metrics_by_grouping_sets(spark, enriched_flights_view)
    
    return agg_df
