import tempfile
from unittest.mock import patch

from es_aws_functions import general_functions

from spp.engine.pipeline import spark_write

logger = general_functions.get_logger(survey="rsi", module_name="SPP Engine - Write",
                                      environment="sandbox", run_id="1111.2222")
suite_location = str(tempfile.gettempdir())


@patch('spp.engine.write.write_spark_df_to_s3')
def test_spark_write_csv(write_to_s3, create_session):
    df = create_session.read.csv('./tests/resources/data/dummy.csv')
    test_location = f"{suite_location}/test_spark_write_file.csv"
    test_target = {
        "location": test_location,
        "format": "csv",
        "save_mode": "append",
        "partition_by": ["_c0"]
    }

    spark_write(df, test_target, counter=0, logger=logger)
    assert write_to_s3.call_args[0][1]['location'] == \
        suite_location + "/test_spark_write_file.csv"


@patch('spp.engine.write.write_spark_df_to_s3')
def test_spark_write_json(write_to_s3, create_session):
    df = create_session.read.json('./tests/resources/data/dummy.json')
    test_location = f"{suite_location}/test_spark_write_file.json"
    test_target = {
        "location": test_location,
        "format": "json",
        "save_mode": "append",
        "partition_by": ["a"]
    }

    spark_write(df, test_target, counter=0, logger=logger)
    assert write_to_s3.call_args[0][1]['location'] == \
        suite_location + "/test_spark_write_file.json"


@patch('spp.engine.write.write_spark_df_to_s3')
def test_spark_write_file_with_partitions(write_to_s3, create_session):
    df = create_session.read.csv('./tests/resources/data/dummy.csv')
    test_location = f"{suite_location}/test_spark_write_file_with_partitions.csv"
    test_target = {
        "location": test_location,
        "format": "parquet",
        "save_mode": "append",
        "partition_by": ["_c0"]
    }

    spark_write(df, test_target, counter=0, logger=logger, partitions=['_c0'])
    assert write_to_s3.call_args[0][1]['location'] == \
        suite_location + "/test_spark_write_file_with_partitions.csv"
