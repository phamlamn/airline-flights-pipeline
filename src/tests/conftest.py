import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope='session')
def spark() -> SparkSession:
    # Yield spark session
    spark = SparkSession.builder \
        .master('local') \
        .appName('Unit-Testing') \
        .getOrCreate()
    return spark
