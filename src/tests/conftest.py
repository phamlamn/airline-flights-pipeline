import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope='session')
def spark() -> SparkSession:
    return SparkSession.builder \
        .master('local') \
        .appName('Unit-Testing') \
        .getOrCreate()