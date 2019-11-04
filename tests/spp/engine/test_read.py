from spp.engine.read import spark_read, pandas_read
from spp.engine.query import Query
import pandas as pd
from pandas.testing import assert_frame_equal
import importlib


def test_spark_read_db(spark_test_session):

    df = spark_test_session.read.csv('./tests/resources/data/dummy.csv')
    df.createOrReplaceGlobalTempView('view')
    assert spark_read(spark_test_session, Query('global_temp', 'view', '*')).collect() == df.collect()


def test_spark_read_csv(spark_test_session):

    df = spark_test_session.read.csv('./tests/resources/data/dummy.csv')
    assert spark_read(spark_test_session, './tests/resources/data/dummy.csv').collect() == df.collect()


def test_spark_read_json(spark_test_session):

    df = spark_test_session.read.json('./tests/resources/data/dummy.json')
    assert spark_read(spark_test_session, './tests/resources/data/dummy.json').collect() == df.collect()


# TODO: Implement this test with a DB-like connection
def test_pandas_read_db():

    def mock_pandas_read(cursor, connection=None):
        return pd.read_csv('./tests/resources/data/dummy.csv')

    orig_pandas_read = getattr(importlib.import_module('spp.engine.read', 'spp.engine'), 'pandas_read')
    try:
        pandas_read = mock_pandas_read
        df = pd.read_csv('./tests/resources/data/dummy.csv')
        assert_frame_equal(pandas_read('./tests/resources/data/dummy.csv', 'connection'), df)
    finally:
        pandas_read = orig_pandas_read


def test_pandas_read_csv():

    df = pd.read_csv('./tests/resources/data/dummy.csv')
    assert_frame_equal(pandas_read('./tests/resources/data/dummy.csv'), df)


def test_pandas_read_json():

    df = pd.read_json('./tests/resources/data/dummy.json', lines=True)
    assert_frame_equal(pandas_read('./tests/resources/data/dummy.json', lines=True), df)
