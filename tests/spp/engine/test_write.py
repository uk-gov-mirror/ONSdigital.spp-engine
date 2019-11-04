from spp.engine.write import spark_write, pandas_write
import pandas as pd
import os
import shutil


suite_location = './tests/tmp'


def test_spark_write_file(spark_test_session):

    df = spark_test_session.read.csv('./tests/resources/data/dummy.csv')
    test_location = f"{suite_location}/test_spark_write_file.csv"
    spark_write(df, test_location)

    assert os.path.exists(test_location)
    shutil.rmtree(suite_location)


def test_spark_write_file_with_partitions(spark_test_session):

    df = spark_test_session.read.csv('./tests/resources/data/dummy.csv')
    test_location = f"{suite_location}/test_spark_write_file_with_partitions.csv"
    spark_write(df, test_location, partitions=['_c0'])

    assert os.path.exists(test_location)
    shutil.rmtree(suite_location)


def test_pandas_write_file():

    df = pd.read_csv('./tests/resources/data/dummy.csv')
    test_location = f"{suite_location}/test_pandas_write_file.csv"

    if not os.path.exists(suite_location):
        os.mkdir(suite_location)
    pandas_write(df, test_location)

    assert os.path.exists(test_location)
    shutil.rmtree(suite_location)
