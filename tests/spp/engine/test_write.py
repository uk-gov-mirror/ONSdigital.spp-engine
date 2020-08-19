from spp.engine.write import spark_write, pandas_write
import pandas as pd
import os
import shutil
from unittest.mock import patch

suite_location = '/tmp'

@patch('spp.engine.write.write_sparkDf_to_s3')
def test_spark_write_csv(write_to_s3, create_session):
    df = create_session.read.csv('./tests/resources/data/dummy.csv')
    test_location = f"{suite_location}/test_spark_write_file.csv"
    test_target = {
        "location": test_location,
        "format": "csv",
        "save_mode": "append",
        "partition_by": ["_c0"]
    }

    spark_write(df, test_target, counter=0)
    assert write_to_s3.call_args[0][1]['location'] == "/tmp/test_spark_write_file.csv"


@patch('spp.engine.write.write_sparkDf_to_s3')
def test_spark_write_json(write_to_s3, create_session):
    df = create_session.read.json('./tests/resources/data/dummy.json')
    test_location = f"{suite_location}/test_spark_write_file.json"
    test_target = {
        "location": test_location,
        "format": "json",
        "save_mode": "append",
        "partition_by": ["a"]
    }

    spark_write(df, test_target, counter=0)
    assert write_to_s3.call_args[0][1]['location'] == "/tmp/test_spark_write_file.json"


@patch('spp.engine.write.write_sparkDf_to_s3')
def test_spark_write_file_with_partitions(write_to_s3, create_session):
    df = create_session.read.csv('./tests/resources/data/dummy.csv')
    test_location = f"{suite_location}/test_spark_write_file_with_partitions.csv"
    test_target = {
        "location": test_location,
        "format": "parquet",
        "save_mode": "append",
        "partition_by": ["_c0"]
    }

    spark_write(df, test_target, counter=0, partitions=['_c0'])
    assert write_to_s3.call_args[0][1]['location'] == "/tmp/test_spark_write_file_with_partitions.csv"

@patch('spp.engine.write.write_pandasDf_to_s3')
def test_pandas_write_parquet(write_to_s3):
    df = pd.read_csv('./tests/resources/data/dummy.csv')
    test_location = f"{suite_location}/test_pandas_write_file.parquet"
    test_target = {
        "location": test_location,
        "format": "parquet",
        "save_mode": "append",
        "partition_by": ["c"]
    }


    if not os.path.exists(suite_location):
        os.mkdir(suite_location)

    pandas_write(df, test_target, counter=0)
    assert write_to_s3.call_args[0][1]['location'] == "/tmp/test_pandas_write_file.parquet"



