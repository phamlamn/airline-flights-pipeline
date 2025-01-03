import pytest
from unittest.mock import MagicMock, ANY
from pydeequ import Check, CheckLevel
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from ..data_quality import *


@pytest.fixture(scope="function")
def mock_check() -> MagicMock:
    """
    Fixture for creating a mock Check object.
    """
    return MagicMock(spec=Check)


def test_UniquenessCheckStrategy_add_check(mock_check):
    """
    Test that UniquenessCheckStrategy.add_check calls hasUniqueness with the correct columns.
    
    Args:
        mock_check: Mock object for the check.
    Asserts:
        hasUniqueness is called with the correct columns.
    """
    table_config = {"uniqueness": ["col1", "col2"]}

    # Call the method under test
    UniquenessCheckStrategy.add_check(mock_check, table_config)

    # Assert that the hasUniqueness method was called with the correct arguments (ignore lambda function)
    mock_check.hasUniqueness.assert_called_with(["col1", "col2"], ANY)


def test_CompletenessCheckStrategy_add_check(mock_check):
    """
    Test that CompletenessCheckStrategy.add_check calls isComplete for each column in table_config.
    
    Args:
        mock_check: Mock object for the check.
    Asserts:
        isComplete is called for each column in table_config["completeness"].
        The call count of isComplete matches the number of columns.
    """
    table_config = {"completeness": ["col1", "col2"]}
    
    # Call the method under test
    CompletenessCheckStrategy.add_check(mock_check, table_config)
    
    # Assert that the isComplete method was called for each column in the table_config
    for col in table_config["completeness"]:
        mock_check.isComplete.assert_any_call(col)

    # Assert that the isComplete method was called same number of times as the number of columns    
    assert mock_check.isComplete.call_count == len(table_config["completeness"])


def test_NonNegativeCheckStrategy_add_check(mock_check):
    """
    Test that NonNegativeCheckStrategy.add_check calls isNonNegative for each column in table_config.
    
    Args:
        mock_check: Mock object for the check.
    Asserts:
        isNonNegative is called for each column in table_config["non_negative"].
        The call count of isNonNegative matches the number of columns.
    """
    table_config = {"non_negative": ["col1", "col2"]}

    # Call the method under test
    NonNegativeCheckStrategy.add_check(mock_check, table_config)

    # Assert that the isNonNegative method was called for each column in the table_config
    for col in table_config["non_negative"]:
        mock_check.isNonNegative.assert_any_call(col)
    
    # Assert that the isNonNegative method was called same number of times as the number of columns    
    assert mock_check.isNonNegative.call_count == len(table_config["non_negative"])


def test_ContainmentCheckStrategy_add_check(mock_check):
    """
    Test that ContainmentCheckStrategy.add_check calls isContainedIn for each column in table_config.
    
    Args:
        mock_check: Mock object for the check.
    Asserts:
        isContainedIn is called for each column in table_config["containment"].
        The call count of isContainedIn matches the number of columns.
    """
    table_config = {"containment": {"col1": ["val1", "val2"], "col2": ["val3", "val4"]}}

    # Call the method under test
    ContainmentCheckStrategy.add_check(mock_check, table_config)

    # Assert that the isContainedIn method was called for each column in the table_config
    for col, values in table_config["containment"].items():
        mock_check.isContainedIn.assert_any_call(col, values)
    
    # Assert that the isContainedIn method was called same number of times as the number of columns    
    assert mock_check.isContainedIn.call_count == len(table_config["containment"])


def test_DataQualityStrategyFactory_get_strategy():
    """
    Test that DataQualityStrategyFactory.get_strategy returns the correct strategies.

    Asserts:
        The correct strategies are returned
    """
    table_config = {
        "uniqueness": ["col1"],
        "completeness": ["col2"],
        "non_negative": ["col3"],
        "containment": {"col4": ["val1", "val2"]}
    }

    strategies = DataQualityStrategyFactory.get_strategy(table_config)

    # Assert that the correct strategies are returned
    assert len(strategies) == 4
    assert UniquenessCheckStrategy in strategies
    assert CompletenessCheckStrategy in strategies
    assert NonNegativeCheckStrategy in strategies
    assert ContainmentCheckStrategy in strategies


@pytest.fixture(scope="module")
def sample_df(spark) -> DataFrame:
    data = [("val1", "val2", 1), ("val3", "val4", 2)]
    columns = ["col1", "col2", "col3"]
    return spark.createDataFrame(data, columns)

@pytest.fixture(scope="module")
def verification_context(spark, sample_df) -> VerificationContext:
    table_config = {
        "uniqueness": ["col1"],
        "completeness": ["col2"],
        "non_negative": ["col3"],
        "containment": {"col1": ["val1", "val3"]}
    }
    return VerificationContext(spark, sample_df, "test_table", table_config)

def test_VerificationContext_add_checks(verification_context):
    """
    Test that VerificationContext.add_checks adds the correct checks.
    
    Args:
        verification_context: Fixture for VerificationContext.
        mock_check: Mock object for the check.
    """
    def get_constraints_from_jvm(check: Check):
        # Get the constraints from the Check object in JVM
        java_constraints = check._Check.constraints()
        # Convert constraints to Java list
        java_list = check._jvm.scala.collection.JavaConverters.seqAsJavaListConverter(java_constraints).asJava()
        # Convert Java list to Python list
        python_list = [str(java_list.get(i)) for i in range(java_list.size())]
        return python_list
    
    # Call the method under test
    verification_context.add_checks()

    # Get the constraints from the Check object in JVM
    constraints = get_constraints_from_jvm(verification_context.check)

    # Assert that the correct constraints have been added
    expected = [
        'UniquenessConstraint(Uniqueness(Stream(col1, ?),None,None))',
        'CompletenessConstraint(Completeness(col2,None,None))',
        'ComplianceConstraint(Compliance(col3 is non-negative,COALESCE(CAST(col3 AS DECIMAL(20,10)), 0.0) >= 0,None,List(col3),None))',
        "ComplianceConstraint(Compliance(col1 contained in val1,val3,`col1` IS NULL OR `col1` IN ('val1','val3'),None,List(col1),None))"
    ]
    assert constraints == expected


# TODO: Uncomment this test after implementing run_verification
# def test_VerificationContext_run_verification(mocker, verification_context):
#     """
#     Test that VerificationContext.run_verification runs the verification and returns the result.
    
#     Args:
#         mocker: Mock object for patching.
#         verification_context: Fixture for VerificationContext.
#     """
#     mock_verification_suite = mocker.patch('data_quality.VerificationSuite')
#     mock_verification_suite.return_value.onData.return_value.addCheck.return_value.run.return_value = "mock_result"

#     result = verification_context.run_verification()

#     # Assert that the verification result is returned
#     assert result == "mock_result"
#     mock_verification_suite.assert_called_once()
#     mock_verification_suite.return_value.onData.assert_called_once_with(verification_context.df)
#     mock_verification_suite.return_value.onData.return_value.addCheck.assert_called_once_with(verification_context.check)
#     mock_verification_suite.return_value.onData.return_value.addCheck.return_value.run.assert_called_once()



# TODO: Uncomment this test after implementing run_data_quality_checks
# def test_run_data_quality_checks(spark, sample_df):
#     table_name = "fact_flights"
#     result = run_data_quality_checks(spark, sample_df, table_name)

#     # Assert that the verification result is returned
#     assert result is not None
