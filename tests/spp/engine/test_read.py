from spp.engine.read import spark_read, pandas_read
from spp.engine.query import Query
import pandas as pd
from pandas.testing import assert_frame_equal


def test_spark_read_db(spark_test_session):

    df = spark_test_session.read.csv('./tests/resources/data/dummy.csv')
    df.createOrReplaceGlobalTempView('view')
    assert spark_read(spark_test_session, Query('global_temp', 'view', '*')).collect() == df.collect()


def test_spark_read_file(spark_test_session):

    df = spark_test_session.read.csv('./tests/resources/data/dummy.csv')
    assert spark_read(spark_test_session, './tests/resources/data/dummy.csv').collect() == df.collect()


def test_pandas_read_file():

    df = pd.read_csv('./tests/resources/data/dummy.csv')
    assert_frame_equal(pandas_read('./tests/resources/data/dummy.csv'), df)


# TODO: Implement this test with a DB-like connection
# def test_pandas_read_db():
#     pass
