from spp.engine import read
from spp.utils.query import Query
import pandas as pd
from pandas.testing import assert_frame_equal
from unittest.mock import MagicMock


def test_spark_read_db(create_session):

    df = create_session.read.csv('./tests/resources/data/dummy.csv')
    df.createOrReplaceGlobalTempView('view')
    assert read.spark_read(create_session, Query('global_temp', 'view', '*')).collect() == df.collect()


def test_spark_read_csv(create_session):

    df = create_session.read.csv('./tests/resources/data/dummy.csv')
    assert read.spark_read(create_session, './tests/resources/data/dummy.csv').collect() == df.collect()


def test_spark_read_json(create_session):

    df = create_session.read.json('./tests/resources/data/dummy.json')
    assert read.spark_read(create_session, './tests/resources/data/dummy.json').collect() == df.collect()


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
