import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope='session')
def spark_test_session(app_name='pytest', master='local'):
    """
    Creates a Spark session to be used throughout all unit tests.
    :param app_name: Application name
    :param master: Master configuration
    :returns SparkSession:
    """
    return SparkSession.builder.appName(app_name).master(master).getOrCreate()
