from pydeequ import *
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite

from pyspark.sql import SparkSession, DataFrame


TABLES_CONFIG = {
    'fact_flights': {
        'uniqueness': ['date', 'airline', 'flight_number', 'scheduled_departure'],
        'completeness': [
            'date', 'airline', 'flight_number', 'origin_airport',
            'destination_airport', 'scheduled_departure', 'scheduled_arrival',
            'distance', 'diverted', 'cancelled', 'is_delayed'
        ],
        'non_negative': [
            'flight_number', 'scheduled_departure', 'departure_time', 'taxi_out', 'wheels_off',
            'scheduled_time', 'elapsed_time', 'air_time', 'distance', 'wheels_on', 'taxi_in',
            'arrival_time', 'air_system_delay', 'security_delay', 'airline_delay', 'late_aircraft_delay'
        ],
        'containment': {
            'airline': ['WN', 'DL', 'AA', 'OO', 'EV', 'UA', 'MQ', 'B6', 'US', 'AS', 'NK', 'F9', 'HA', 'VX'],
            'diverted': ['0', '1'],
            'cancelled': ['0', '1'],
            'cancellation_reason': ['A', 'B', 'C', 'D', ''],
            'is_delayed': ['0', '1']
        }
    },
    
    'dim_airlines': {},
    
    'dim_airports': {},
    
    'dim_cancellation_reasons': {},
    
    'dim_dates': {}
}


class CheckStrategy:
    """Base interface"""
    def add_check(
        self,
        spark: SparkSession,
        check: Check,
        table_config: dict
    ) -> Check:
        raise NotImplementedError("Subclasses must implement this method")

class UniquenessCheckStrategy(CheckStrategy):
    def add_check(
        self,
        spark: SparkSession,
        check: Check,
        table_config: dict
    ) -> Check:
        
        unique_cols = table_config['uniqueness']
        return check.hasUniqueness(unique_cols, lambda x: x == 1.0)

class CompletenessCheckStrategy(CheckStrategy):
    def add_check(
        self,
        spark: SparkSession,
        check: Check,
        table_config: dict
    ) -> Check:
        
        for col in table_config['completeness']:
            check.isComplete(col)
        return check

class NonNegativeCheckStrategy(CheckStrategy):
    def add_check(
        self,
        spark: SparkSession,
        check: Check,
        table_config: dict
    ) -> Check:
        
        for col in table_config['non_negative']:
            check.isNonNegative(col)
        return check
    
class ContainmentCheckStrategy(CheckStrategy):
    def add_check(
        self,
        spark: SparkSession,
        check: Check,
        table_config: dict
    ) -> Check:
        
        for col, values in table_config['containment'].items():
            check.isContainedIn(col, values)
        return check  


class DataQualityStrategyFactory:
    STRATEGY_MAPPING = {
        'uniqueness': UniquenessCheckStrategy,
        'completeness': CompletenessCheckStrategy,
        'non_negative': NonNegativeCheckStrategy,
        'containment': ContainmentCheckStrategy
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
    def __init__(
        self,spark: SparkSession,
        df: DataFrame,
        table_name: str,
        table_config: dict
    ):
        self.spark = spark
        self.df = df
        self.table_config = table_config
        self.check = Check(spark, CheckLevel.Error, f"Data Quality Checks - {table_name}")
        self.strategies = DataQualityStrategyFactory.get_strategy(table_config)
        
    def add_checks(self):
        for strategy in self.strategies:
            self.check = strategy.add_check(self.spark, self.check, self.table_config)
        return self.check
    
    def run_verification(self):
        result = VerificationSuite(self.spark) \
            .onData(self.df) \
            .addCheck(self.check) \
            .run()
        return result.run()


def run_data_quality_checks(spark: SparkSession, df: DataFrame, table_name: str):
    """
    Run data quality checks for a given table and DataFrame.

    Args:
        spark: Spark session.
        table_name: Name of the table.
        dataframe: DataFrame to check.

    Returns:
        Verification result.
    """
    if table_name not in TABLES_CONFIG:
        raise ValueError(f"Table {table_name} not found in TABLE_CONFIG.")
    
    table_config = TABLES_CONFIG[table_name]

    context = VerificationContext(spark, df, table_name, table_config)
    result = context.add_checks().run_verification()
    
    # Print results
    if result.status == 'Success':
        print(f"Data quality checks passed for {table_name}")
    else:
        print(f"Data quality checks failed for {table_name}")
        for constraint in result.checkResults:
            print(f"- {constraint.constraint}: {constraint.constraint_status}")

    return result






def temp():
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