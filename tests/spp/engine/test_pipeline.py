# Mock Data Access for all tests need to return dataframes for each pipeline
# 1.  Create a new AWS small data PipelineMethod
# 2.  Run the method
# 3,  Create a new AWS Spark PipelineMethod
# 4.  Run the method
# 5.  Create a new AWS small data pipeline
# 6.  Add methods to the pipeline
# 7.  Run the pipeline
# 8.  Create a new AWS Spark Pipeline
# 9.  Add methods to the pipeline
# 10. Run the pipeline

import pytest
from mock import patch, PropertyMock
from spp.engine.pipeline import PipelineMethod, Platform, Pipeline
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, BooleanType
import pandas as pd


@patch('spp.engine.pipeline.write_data')
@patch('spp.engine.pipeline.DataAccess')
def test_aws_small_method(mock_class, mock_method):

    mock_method.return_value("Data has been written out")
    mock_class.return_value.name = "df"
    mock_class().read_data.return_value = pd.DataFrame({"old_col": pd.Series([1])})

    test_method = PipelineMethod("method_c", "tests.test_methods.sd.small_data",
                                 {"df": "DataFrame"},
                                 {"param_1": 1, "param_2": "New_col", "param_3": ["value"]})

    test_method.run(Platform.AWS, None)


@patch('spp.engine.pipeline.write_data')
@patch('spp.engine.pipeline.DataAccess')
def test_aws_big_method(mock_class, mock_method, create_session):

    mock_method.return_value("Data has been written out")
    mock_class.return_value.name = "df"

    schema = StructType([
        StructField("id", StringType(), nullable=False),
        StructField("short_id", IntegerType(), nullable=False)
    ])

    data = [("000001", 1), ("000002", 2)]

    sdf = create_session.createDataFrame(data, schema)

    mock_class().read_data.return_value = sdf

    test_method = PipelineMethod("method_a", "tests.test_methods.bd.big_data",
                                 {"df": "DataFrame"},
                                 {"param_1": "col_1", "param_2": "col_2", "param_3": "col_3"})

    test_method.run(Platform.AWS, create_session)


@patch('spp.engine.pipeline.write_data')
@patch('spp.engine.pipeline.DataAccess')
def test_aws_small_pipeline(mock_class, mock_method):

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
    mock_class().read_data.side_effect = dfs

    test_pipeline = Pipeline("Test", Platform.AWS, False)

    test_pipeline.add_pipeline_methods("method_c", "tests.test_methods.sd.small_data",
                                       {"df": "DataFrame"},
                                       {"param_1": 1, "param_2": "New_col", "param_3": ["value", "value_2"]})

    test_pipeline.add_pipeline_methods("method_d", "tests.test_methods.sd.small_data",
                                       {"df_1": "DataFrame_1", "df_2": "DataFrame_2"},
                                       {"param_1": "reporting_date", "param_2": "entity_name"})

    test_pipeline.run(Platform.AWS)


@patch('spp.engine.pipeline.write_data')
@patch('spp.engine.pipeline.DataAccess')
def test_aws_big_pipeline(mock_class, mock_method, create_session):

    df_names = ["df", "df_1", "df_2"]

    schema = StructType([
        StructField("id", StringType(), nullable=False),
        StructField("short_id", IntegerType(), nullable=False)
    ])

    data = [("000001", 1), ("000002", 2)]


    schema_1 =  StructType([
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
    mock_class().read_data.side_effect = dfs

    test_pipeline = Pipeline("Test", Platform.AWS, True)

    test_pipeline.add_pipeline_methods("method_a", "tests.test_methods.bd.big_data",
                                       {"df": "DataFrame"},
                                       {"param_1": "col_1", "param_2": "col_2", "param_3": "col_3"})

    test_pipeline.add_pipeline_methods("method_b", "tests.test_methods.bd.big_data",
                                       {"df_1": "DataFrame_1", "df_2": "DataFrame_2"},
                                       {"param_1": "reporting_date", "param_2": "entity_name"})

    test_pipeline.run(Platform.AWS)


