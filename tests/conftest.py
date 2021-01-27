import os

import pytest

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def create_session(request):
    os.environ["LOGGING_LEVEL"] = "DEBUG"
    return SparkSession.builder.master("local[*]").appName("Test").getOrCreate()
