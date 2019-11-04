from spp.engine.read import read_db, read_file
from pyspark.sql import SparkSession
import pandas as pd
from pandas.testing import assert_frame_equal


def test_spark_read_db():

    spark = SparkSession.builder.appName('Test').master('local').getOrCreate()
    df = spark.read.csv('./tests/resources/data/dummy.csv')
    df.createOrReplaceGlobalTempView('view')

    assert read_db(spark, 'global_temp', 'view', '*').collect() == df.collect()


def test_spark_read_file():

    spark = SparkSession.builder.appName('Pytest').master('local').getOrCreate()
    df = spark.read.csv('./tests/resources/data/dummy.csv')
    df.show()
    read_file('./tests/resources/data/dummy.csv', spark).show()


    assert read_file('./tests/resources/data/dummy.csv', spark).collect() == df.collect()


def test_pandas_read_file():

    df = pd.read_csv('./tests/resources/data/dummy.csv')
    assert_frame_equal(read_file('./tests/resources/data/dummy.csv'), df)


# TODO: Implement this test with a DB-like connection
# def test_pandas_read_db():
#     pass
