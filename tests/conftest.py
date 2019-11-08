import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def create_session(request):

    return SparkSession.builder.appName("Test").getOrCreate()