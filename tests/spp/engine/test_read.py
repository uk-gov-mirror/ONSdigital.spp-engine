from spp.engine.read import read_db, read_file
import pandas as pd
from pandas.testing import assert_frame_equal


def test_spark_read_db(spark_test_session):

    df = spark_test_session.read.csv('./tests/resources/data/dummy.csv')
    df.createOrReplaceGlobalTempView('view')
    assert read_db(spark_test_session, 'global_temp', 'view', '*').collect() == df.collect()


def test_spark_read_file(spark_test_session):

    df = spark_test_session.read.csv('./tests/resources/data/dummy.csv')
    assert read_file('./tests/resources/data/dummy.csv', spark_test_session).collect() == df.collect()


def test_pandas_read_file():

    df = pd.read_csv('./tests/resources/data/dummy.csv')
    assert_frame_equal(read_file('./tests/resources/data/dummy.csv'), df)


# TODO: Implement this test with a DB-like connection
# def test_pandas_read_db():
#     pass
