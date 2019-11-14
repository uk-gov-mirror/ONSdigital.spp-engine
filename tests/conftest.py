import pytest
from pyspark.sql import SparkSession
import os


@pytest.fixture(scope="session")
def create_session(request):
    os.environ["LOGGING_LEVEL"] = "DEBUG"
    return SparkSession.builder.appName("Test").getOrCreate()