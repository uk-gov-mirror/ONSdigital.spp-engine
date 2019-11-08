from spp.engine.write import spark_write, pandas_write
import pandas as pd
import os
import shutil


suite_location = './tests/tmp'


def test_spark_write_csv(create_session):

    df = create_session.read.csv('./tests/resources/data/dummy.csv')
    test_location = f"{suite_location}/test_spark_write_file.csv"

    try:
        spark_write(df, test_location)
        assert os.path.exists(test_location)
    finally:
        shutil.rmtree(suite_location)


def test_spark_write_json(create_session):

    df = create_session.read.json('./tests/resources/data/dummy.json')
    test_location = f"{suite_location}/test_spark_write_file.json"

    try:
        spark_write(df, test_location)
        assert os.path.exists(test_location)
    finally:
        shutil.rmtree(suite_location)


def test_spark_write_file_with_partitions(create_session):

    df = create_session.read.csv('./tests/resources/data/dummy.csv')
    test_location = f"{suite_location}/test_spark_write_file_with_partitions.csv"

    try:
        spark_write(df, test_location, partitions=['_c0'])
        assert os.path.exists(test_location)
    finally:
        shutil.rmtree(suite_location)


def test_pandas_write_csv():

    df = pd.read_csv('./tests/resources/data/dummy.csv')
    test_location = f"{suite_location}/test_pandas_write_file.csv"

    try:
        if not os.path.exists(suite_location):
            os.mkdir(suite_location)
        pandas_write(df, test_location)
        assert os.path.exists(test_location)
    finally:
        shutil.rmtree(suite_location)


def test_pandas_write_json():

    df = pd.read_json('./tests/resources/data/dummy.json', lines=True)
    test_location = f"{suite_location}/test_pandas_write_file.json"

    try:
        if not os.path.exists(suite_location):
            os.mkdir(suite_location)
        pandas_write(df, test_location)
        assert os.path.exists(test_location)
    finally:
        shutil.rmtree(suite_location)
