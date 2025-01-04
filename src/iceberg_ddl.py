import os

CATALOG_NAME = os.environ['CATALOG_NAME']
DATABASE_NAME = os.environ['DATABASE_NAME']


fact_flights_ddl = f"""
CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{DATABASE_NAME}.fact_flights (
    date DATE,
    airline STRING,
    flight_number INT,
    tail_number STRING,
    origin_airport STRING,
    destination_airport STRING,
    scheduled_departure INT,
    departure_time INT,
    departure_delay INT,
    taxi_out INT,
    wheels_off INT,
    scheduled_time INT,
    elapsed_time INT,
    air_time INT,
    distance INT,
    wheels_on INT,
    taxi_in INT,
    scheduled_arrival INT,
    arrival_time INT,
    arrival_delay INT,
    diverted INT,
    cancelled INT,
    cancellation_reason STRING,
    air_system_delay INT,
    security_delay INT,
    airline_delay INT,
    late_aircraft_delay INT,
    weather_delay INT,
    is_delayed INT
)
USING iceberg
PARTITIONED BY (month(date))
"""


dim_airlines_ddl = f"""
CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{DATABASE_NAME}.dim_airlines (
    IATA_CODE STRING,
    AIRLINE STRING
)
USING iceberg
"""


dim_airports_ddl = f"""
CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{DATABASE_NAME}.dim_airports (
    IATA_CODE STRING,
    AIRPORT STRING,
    CITY STRING,
    STATE STRING,
    COUNTRY STRING,
    LATITUDE FLOAT,
    LONGITUDE FLOAT
)
USING iceberg
"""


dim_cancel_codes_ddl = f"""
CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{DATABASE_NAME}.dim_cancellation_codes (
    CANCELLATION_REASON STRING,
    CANCELLATION_DESCRIPTION STRING
)
USING iceberg
"""

dim_dates_ddl = f"""
CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{DATABASE_NAME}.dim_dates (
    date DATE,
    year INT,
    month INT,
    day INT,
    day_of_week INT,
    quarter INT,
    holiday_name STRING,
    is_holiday BOOLEAN
)
"""


# TODO
agg_fact_flights_ddl = f"""
"""


update_fact_flights_ddl = """
MERGE INTO {CATALOG_NAME}.{DATABASE_NAME}.fact_flights t
USING {input_view_name} s
    ON  t.date = s.date
    AND t.airline = s.airline
    AND t.flight_number = s.flight_number
    AND t.scheduled_departure = s.scheduled_departure
WHEN NOT MATCHED THEN INSERT *
"""