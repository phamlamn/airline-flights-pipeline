from pydeequ import *
from pydeequ.checks import Check, CheckLevel
from pydeeqy.verification import VerificationSuite

class DataQualityDefinitions:
    pass

class FlightsDataQualityDefinitions:
    pass


# Uniqueness Checks
uniqueness_check = Check(spark, CheckLevel.Error, "Uniqueness Checks") \
    .hasUniqueness(["date", "airline", "flight_number", "scheduled_departure"], lambda x: x == 1.0)

# Completeness Checks
completeness_check = Check(spark, CheckLevel.Error, "Completeness Checks") \
    .isComplete("date") \
    .isComplete("airline") \
    .isComplete("flight_number") \
    .isComplete("origin_airport") \
    .isComplete("destination_airport") \
    .isComplete("scheduled_departure") \
    .isComplete("scheduled_arrival") \
    .isComplete("distance") \
    .isComplete("diverted") \
    .isComplete("cancelled") \
    .isComplete("is_delayed")

# Non-Negative Checks
non_negative_check = Check(spark, CheckLevel.Error, "Non-Negative Checks") \
    .isNonNegative("flight_number") \
    .isNonNegative("scheduled_departure") \
    .isNonNegative("departure_time") \
    .isNonNegative("taxi_out") \
    .isNonNegative("wheels_off") \
    .isNonNegative("scheduled_time") \
    .isNonNegative("elapsed_time") \
    .isNonNegative("air_time") \
    .isNonNegative("distance") \
    .isNonNegative("wheels_on") \
    .isNonNegative("taxi_in") \
    .isNonNegative("arrival_time") \
    .isNonNegative("air_system_delay") \
    .isNonNegative("security_delay") \
    .isNonNegative("airline_delay") \
    .isNonNegative("late_aircraft_delay") \
    .isNonNegative("weather_delay")

# Containment Checks
containment_check = Check(spark, CheckLevel.Error, "Containment Checks") \
    .isContainedIn("airline", ["WN", "DL", "AA", "OO", "EV", "UA", "MQ", "B6", "US", "AS", "NK", "F9", "HA", "VX"]) \
    .isContainedIn("diverted", ["0", "1"]) \
    .isContainedIn("cancelled", ["0", "1"]) \
    .isContainedIn("cancellation_reason", ["A", "B", "C", "D", ""]) \
    .isContainedIn("is_delayed", ["0", "1"])


# Define verification suite
verification_suite = VerificationSuite(spark) \
    .onData(dataframe) \
    .addCheck(uniqueness_check) \
    .addCheck(completeness_check) \
    .addCheck(non_negative_check) \
    .addCheck(containment_check)

# Run verification suite
result = verification_suite.run()