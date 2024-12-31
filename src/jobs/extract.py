from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, expr, when, to_date, year, month, day, dayofweek, quarter, broadcast
from pyspark.sql.types import StructType


def read_csv(
    spark: SparkSession,
    filename: str,
    schema: StructType
) -> DataFrame:
    return spark.read \
                .schema(schema) \
                .option('header', True) \
                .csv(filename)


# TODO Generalize to any year
def generate_dim_date_df(spark: SparkSession) -> DataFrame:
    # Initialize date_df
    date_df = spark.range(365) \
        .withColumn('date', expr('date_add("2015-01-01", CAST(id AS INT))')) \
        .withColumn('year', year('date')) \
        .withColumn('month', month('date')) \
        .withColumn('day', day('date')) \
        .withColumn('day_of_week', dayofweek('date')) \
        .withColumn('quarter', quarter('date')) \
        .drop('id')

    # List of U.S. federal holidays
    us_holidays_2015 = [
        ("2015-01-01", "New Year's Day"),
        ("2015-01-19", "Martin Luther King Jr. Day"),
        ("2015-02-16", "Presidents' Day"),
        ("2015-05-25", "Memorial Day"),
        ("2015-07-04", "Independence Day"),
        ("2015-09-07", "Labor Day"),
        ("2015-10-12", "Columbus Day"),
        ("2015-11-11", "Veterans Day"),
        ("2015-11-26", "Thanksgiving Day"),
        ("2015-12-25", "Christmas Day"),
    ]
    
    # Create holidays_df and cast date from STRING to DATE type
    holidays_df = spark.createDataFrame(us_holidays_2015, ['holiday_date', 'holiday_name'])
    holidays_df = holidays_df.withColumn('holiday_date', to_date('holiday_date'))

    # Join holidays to date_df and add is_holiday column
    date_df = date_df \
        .join(
            broadcast(holidays_df),
            date_df.date == holidays_df.holiday_date,
            'left'
        ) \
        .withColumn(
            'is_holiday',
            when(col('holiday_name').isNotNull(), lit(True)).otherwise(lit(False))
        ) \
        .drop('holiday_date') \
        .sort('date')

    # Rearrange date to be first column,
    date_df = date_df.select('date', *[col(c) for c in date_df.columns if c != 'date'])
    return date_df
