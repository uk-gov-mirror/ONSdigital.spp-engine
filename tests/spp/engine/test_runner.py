# parse json data
# validate json data
# mock data access
# check pipeline runs
import pandas as pd

import json
from unittest.mock import patch, PropertyMock
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, BooleanType
from pyspark.sql import SparkSession

from scripts.runner import Runner
#from spp.engine.pipeline import Platform


with open("./tests/resources/config/test_bd_pipeline.json") as f:
    test_bd_json = json.load(f)

with open("./tests/resources/config/test_sd_pipeline.json") as f:
    test_sd_json = json.load(f)


@patch('spp.engine.pipeline.write_data')
@patch('spp.engine.pipeline.DataAccess')
def test_parse_config_bd(mock_class, mock_method,create_session):

    runner = Runner(test_bd_json)
    pipeline = runner.pipeline
    df_names = ["df", "df_1", "df_2"]

    schema = StructType([
        StructField("id", StringType(), nullable=False),
        StructField("short_id", IntegerType(), nullable=False)
    ])

    data = [("000001", 1), ("000002", 2)]

    schema_1 = StructType([
        StructField("reporting_date", StringType(), nullable=True),
        StructField("entity_name", StringType(), nullable=True),
        StructField("value", IntegerType(), nullable=True)
    ])

    schema_2 = StructType([
        StructField("reporting_date", StringType(), nullable=True),
        StructField("entity_name", StringType(), nullable=True),
        StructField("valid", BooleanType(), nullable=True)
    ])

    data_1 = [("201602", "test_name", 1000), ("201603", "not_test_name", 1000), ("201604", "test_name", 1000)]
    data_2 = [("201602", "test_name", True), ("201603", "not_test_name", False)]

    dfs = [create_session.createDataFrame(data, schema), create_session.createDataFrame(data_1, schema_1),
           create_session.createDataFrame(data_2, schema_2)]

    mock_method.return_value("Data has been written out")
    type(mock_class()).name = PropertyMock(side_effect=df_names)
    mock_class().pipeline_read_data.side_effect = dfs


    assert runner.run_id == '000001'
    assert pipeline.name == 'test_pipeline'
    assert isinstance(pipeline.spark, SparkSession)
    assert pipeline.platform == "AWS"

    assert pipeline.methods[0].module_name == 'tests.test_methods.bd.big_data'
    assert pipeline.methods[0].method_name == 'method_a'
    assert pipeline.methods[0].params == {
        "param_1": "col_1",
        "param_2": "col_2",
        "param_3": "col_3"
    }

    assert pipeline.methods[1].module_name == 'tests.test_methods.bd.big_data'
    assert pipeline.methods[1].method_name == 'method_b'
    assert pipeline.methods[1].params == {
        "param_1": "reporting_date",
        "param_2": "entity_name"
    }

    # TODO: Add DataAccess parsing

    # with patch.multiple(
    #     'spp.engine.data_access.DataAccess.pipeline_read_data',
    #     return_value=pipeline.spark.read.json('./tests/resources/data/dummy2.json')
    # ):
    #     runner.run()
    #runner.run()
    #mock_pipeline_read_data.assert_called_once_with(runner)
    #mock_write_data.assert_called_once_with(runner)
    runner.run()

@patch('spp.engine.pipeline.write_data')
@patch('spp.engine.pipeline.DataAccess')
def test_parse_config_sd(mock_class, mock_method):

    runner = Runner(test_sd_json)
    pipeline = runner.pipeline
    df_names = ["df", "df_1", "df_2"]
    dfs = [pd.DataFrame({"old_col": pd.Series([1])}),
           pd.DataFrame({"reporting_date": pd.Series(["201602", "201603", "201604"]), "entity_name": pd.Series([
               "test_name", "not_test_name", "test_name"]), 'value': pd.Series([1000] * 3)}),
           pd.DataFrame({"reporting_date": pd.Series(["201602", "201603"]), "entity_name": pd.Series(["test_name",
                                                                                                      "not_test_name"]),
                         "valid": pd.Series([True, False])}
                        )]

    mock_method.return_value("Data has been written out")
    type(mock_class()).name = PropertyMock(side_effect=df_names)
    mock_class().pipeline_read_data.side_effect = dfs

    #mock_method.return_value("Data has been written out")
    assert runner.run_id == '000002'
    assert pipeline.name == 'test_sd_pipeline'
    assert not pipeline.spark
    assert pipeline.platform == "AWS"

    assert pipeline.methods[0].module_name == 'tests.test_methods.sd.small_data'
    assert pipeline.methods[0].method_name == 'method_c'
    assert pipeline.methods[0].params == {
        "param_1": "col_1",
        "param_2": "col_2",
        "param_3": "col_3"
    }

    assert pipeline.methods[1].module_name == 'tests.test_methods.sd.small_data'
    assert pipeline.methods[1].method_name == 'method_d'
    assert pipeline.methods[1].params == {
        "param_1": "reporting_date",
        "param_2": "entity_name"
    }
    runner.run()