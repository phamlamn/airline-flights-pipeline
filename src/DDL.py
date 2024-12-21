import os

CATALOG_NAME = os.environ['CATALOG_NAME']
DATABASE_NAME = os.environ['DATABASE_NAME']

# TODO update date columns
flights_ddl = f"""
CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{DATABASE_NAME}.flights (
    year INT,
    month INT,
    day INT,
    day_of_week INT,
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
    weather_delay INT
)
USING iceberg
PARTITIONED BY (month)"""


airlines_ddl = f"""
CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{DATABASE_NAME}.airlines (
    IATA_CODE STRING,
    AIRLINE STRING
)
USING iceberg
"""


airports_ddl = f"""
CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{DATABASE_NAME}.airports (
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


cancellation_codes_ddl = f"""
CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{DATABASE_NAME}.cancellation_codes (
    CANCELLATION_REASON STRING,
    CANCELLATION_DESCRIPTION STRING
)
USING iceberg
"""
