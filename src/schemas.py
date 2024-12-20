from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Define source flights schema
flights_schema = StructType([
    StructField("year", IntegerType(), False),   # Non-Nullable
    StructField("month", IntegerType(), False),  # Non-Nullable
    StructField("day", IntegerType(), False),    # Non-Nullable
    StructField("day_of_week", IntegerType(), False),    # Non-Nullable
    StructField("airline", StringType(), False),         # Non-Nullable
    StructField("flight_number", IntegerType(), False),  # Non-Nullable
    StructField("tail_number", StringType(), True),
    StructField("origin_airport", StringType(), False),  # Non-Nullable
    StructField("destination_airport", StringType(), False),   # Non-Nullable
    StructField("scheduled_departure", IntegerType(), False),  # Non-Nullable
    StructField("departure_time", IntegerType(), True),
    StructField("departure_delay", IntegerType(), True),
    StructField("taxi_out", IntegerType(), True),
    StructField("wheels_off", IntegerType(), True),
    StructField("scheduled_time", IntegerType(), True),
    StructField("elapsed_time", IntegerType(), True),
    StructField("air_time", IntegerType(), True),
    StructField("distance", IntegerType(), False),  # Non-Nullable
    StructField("wheels_on", IntegerType(), True),
    StructField("taxi_in", IntegerType(), True),
    StructField("scheduled_arrival", IntegerType(), False),  # Non-Nullable
    StructField("arrival_time", IntegerType(), True),
    StructField("arrival_delay", IntegerType(), True),
    StructField("diverted", IntegerType(), False),   # Non-Nullable
    StructField("cancelled", IntegerType(), False),  # Non-Nullable
    StructField("cancellation_reason", StringType(), True),
    StructField("air_system_delay", IntegerType(), True),
    StructField("security_delay", IntegerType(), True),
    StructField("airline_delay", IntegerType(), True),
    StructField("late_aircraft_delay", IntegerType(), True),
    StructField("weather_delay", IntegerType(), True)
])


# Define Silver-level flights schema?

# Define airports schema

# Define cancellation_codes schema (do we just denormalize this?)

# Define airlines schema (do we just denormalize this?)