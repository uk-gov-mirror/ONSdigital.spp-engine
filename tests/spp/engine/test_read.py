from spp.engine.read import read_db, read_file
from pyspark.sql import SparkSession
import pandas as pd
from pandas.testing import assert_frame_equal


# def test_spark_read_db():
#
#     spark = SparkSession.builder.appName('Test').master('local').getOrCreate()
#     df = spark.read.json('./tests/resources/data/dummy.csv')
#     df.createOrReplaceGlobalTempView('view')
#
#     assert read_db(spark, 'global_temp', 'view', '*').collect() == df.collect()
#
#
# def test_spark_read_file():
#
#     spark = SparkSession.builder.appName('Test').master('local').getOrCreate()
#     df = spark.read.json('./tests/resources/data/dummy.csv')
#
#     assert read_file(spark, './tests/resources/data/dummy.csv').collect() == df.collect()


def test_pandas_read_file():

    df = pd.read_csv('./tests/resources/data/dummy.csv')
    assert_frame_equal(read_file('./tests/resources/data/dummy.csv'), df)
