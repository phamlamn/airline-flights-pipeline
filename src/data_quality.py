from pydeequ import *
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult

from pyspark.sql import SparkSession, DataFrame


TABLES_CONFIG = {
    "fact_flights": {
        "uniqueness": ["date", "airline", "flight_number", "scheduled_departure"],
        "completeness": [
            "date",
            "airline",
            "flight_number",
            "origin_airport",
            "destination_airport",
            "scheduled_departure",
            "scheduled_arrival",
            "distance",
            "diverted",
            "cancelled",
            "is_delayed",
        ],
        "non_negative": [
            "flight_number",
            "scheduled_departure",
            "departure_time",
            "taxi_out",
            "wheels_off",
            "scheduled_time",
            "elapsed_time",
            "air_time",
            "distance",
            "wheels_on",
            "taxi_in",
            "arrival_time",
            "air_system_delay",
            "security_delay",
            "airline_delay",
            "late_aircraft_delay",
        ],
        "containment": {
            "airline": [
                "WN",
                "DL",
                "AA",
                "OO",
                "EV",
                "UA",
                "MQ",
                "B6",
                "US",
                "AS",
                "NK",
                "F9",
                "HA",
                "VX",
            ],
            "diverted": ["0", "1"],
            "cancelled": ["0", "1"],
            "cancellation_reason": ["A", "B", "C", "D", ""],
            "is_delayed": ["0", "1"],
        },
    },
    
    "dim_airlines": {},
    "dim_airports": {},
    "dim_cancellation_reasons": {},
    "dim_dates": {},
    
    # TODO finalize data quality checks(also update design specification)
    "agg_fact_flights": {
        "completeness": [
            "total_flights",
            "delayed_flights",
            "cancelled_flights",
            "cancellations_A",
            "cancellations_B",
            "cancellations_C",
            "cancellations_D",
            "time_agg_level",
            "agg_level"
        ],
        'non_negative': [
            "total_flights",
            "delayed_flights",
            "delayed_rate",
            "cancelled_flights",
            "cancelled_rate",
            "cancellations_A",
            "cancellations_B",
            "cancellations_C",
            "cancellations_D",
            "percent_cancellations_A",
            "percent_cancellations_B",
            "percent_cancellations_C",
            "percent_cancellations_D"
        ],
        "containment": {
            "time_agg_level": ["all", "year", "month", "year_month", "day_of_week"],
            # "agg_level": ["all", "airport", "origin_airport"]
        }
    }
}


class CheckStrategy:
    """Base interface"""

    @staticmethod
    def add_check(check: Check, table_config: dict) -> Check:
        raise NotImplementedError("Subclasses must implement this method")


class UniquenessCheckStrategy(CheckStrategy):
    @staticmethod
    def add_check(check: Check, table_config: dict) -> Check:
        unique_cols = table_config["uniqueness"]
        return check.hasUniqueness(unique_cols, lambda x: x == 1.0)


class CompletenessCheckStrategy(CheckStrategy):
    @staticmethod
    def add_check(check: Check, table_config: dict) -> Check:
        for col in table_config["completeness"]:
            check.isComplete(col)
        return check


class NonNegativeCheckStrategy(CheckStrategy):
    @staticmethod
    def add_check(check: Check, table_config: dict) -> Check:
        for col in table_config["non_negative"]:
            check.isNonNegative(col)
        return check


class ContainmentCheckStrategy(CheckStrategy):
    @staticmethod
    def add_check(check: Check, table_config: dict) -> Check:
        for col, values in table_config["containment"].items():
            check.isContainedIn(col, values)
        return check


# TODO add CheckStrategies (see agg_fact_flights spec) and add unit tests!

class DataQualityStrategyFactory:
    STRATEGY_MAPPING = {
        "uniqueness": UniquenessCheckStrategy,
        "completeness": CompletenessCheckStrategy,
        "non_negative": NonNegativeCheckStrategy,
        "containment": ContainmentCheckStrategy,
    }

    @staticmethod
    def get_strategy(table_config: dict) -> CheckStrategy:
        strategies = []
        for key, strategy_class in DataQualityStrategyFactory.STRATEGY_MAPPING.items():
            if key in table_config:
                strategies.append(strategy_class)
        return strategies


class VerificationContext:
    """Context class to manage the execution of data quality checks."""

    def __init__(self, spark: SparkSession, df: DataFrame, table_name: str, table_config: dict):
        self.spark = spark
        self.df = df
        self.table_config = table_config
        self.check = Check(spark, CheckLevel.Error, f"Data Quality Checks - {table_name}")
        self.strategies = DataQualityStrategyFactory.get_strategy(table_config)

    def add_checks(self):
        # Add checks for each strategy to Check object
        for strategy in self.strategies:
            self.check = strategy.add_check(self.check, self.table_config)

    def run_verification(self):
        # Run verification and return result
        result = VerificationSuite(self.spark) \
            .onData(self.df) \
            .addCheck(self.check) \
            .run()
        # Perform cleanup
        self.clean_up()
        return result
    
    def clean_up(self):
        # Clean up PyDeequ resources
        self.spark.sparkContext._gateway.shutdown_callback_server()


def run_data_quality_checks(spark: SparkSession, df: DataFrame, table_name: str) -> VerificationResult:
    """
    Run data quality checks for a given table and DataFrame.

    Args:
        spark: Spark session.
        table_name: Name of the table.
        dataframe: DataFrame to check.

    Returns:
        Verification result.
    """
    # Check if valid table name
    if table_name not in TABLES_CONFIG:
        raise ValueError(f"Table {table_name} not found in TABLE_CONFIG.")

    table_config = TABLES_CONFIG[table_name]

    # Initiate context and add checks
    context = VerificationContext(spark, df, table_name, table_config)
    context.add_checks()

    # Run verification
    result = context.run_verification()

    # Print results
    if result.status == "Success":
        print(f"Data quality checks passed for {table_name}")
    else:
        print(f"Data quality checks failed for {table_name}")
        VerificationResult.checkResultsAsDataFrame(spark, result).show(truncate=False)

    return result
