from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import to_date, concat_ws, when, col, lit


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


# TODO
def do_agg_fact_flights_transformation(spark: SparkSession):
    # Read flights table
    
    # Read other tables?
    
    # Join flights with dims
    
    # Perform rollup
    pass

