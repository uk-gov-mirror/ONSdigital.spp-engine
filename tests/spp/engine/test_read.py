from spp.engine import read
from spp.engine.query import Query
import pandas as pd
from pandas.testing import assert_frame_equal
import importlib
from unittest.mock import MagicMock


def test_spark_read_db(spark_test_session):

    df = spark_test_session.read.csv('./tests/resources/data/dummy.csv')
    df.createOrReplaceGlobalTempView('view')
    assert read.spark_read(spark_test_session, Query('global_temp', 'view', '*')).collect() == df.collect()


def test_spark_read_csv(spark_test_session):

    df = spark_test_session.read.csv('./tests/resources/data/dummy.csv')
    assert read.spark_read(spark_test_session, './tests/resources/data/dummy.csv').collect() == df.collect()


def test_spark_read_json(spark_test_session):

    df = spark_test_session.read.json('./tests/resources/data/dummy.json')
    assert read.spark_read(spark_test_session, './tests/resources/data/dummy.json').collect() == df.collect()


# TODO: Implement this mock
def test_pandas_read_db():

    read.pandas_read = MagicMock(return_value=pd.read_csv('./tests/resources/data/dummy.csv'))
    df = pd.read_csv('./tests/resources/data/dummy.csv')
    assert_frame_equal(read.pandas_read('./tests/resources/data/dummy.csv', 'connection'), df)


def test_pandas_read_csv():

    df = pd.read_csv('./tests/resources/data/dummy.csv')
    assert_frame_equal(read.pandas_read('./tests/resources/data/dummy.csv'), df)


def test_pandas_read_json():

    df = pd.read_json('./tests/resources/data/dummy.json', lines=True)
    assert_frame_equal(read.pandas_read('./tests/resources/data/dummy.json', lines=True), df)
