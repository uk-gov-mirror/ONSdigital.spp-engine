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

from unittest.mock import PropertyMock, patch

from es_aws_functions import general_functions

from pyspark.sql.types import (BooleanType, IntegerType, StringType, StructField,
                               StructType)
from spp.engine.pipeline import Pipeline, PipelineMethod

logger = general_functions.get_logger(survey="rsi", module_name="SPP Engine - Write",
                                      environment="sandbox", run_id="1111.2222")


@patch('spp.engine.pipeline.crawl')
@patch('spp.engine.pipeline.write_data')
@patch('spp.engine.pipeline.DataAccess')
def test_aws_big_method(mock_class, mock_method, crawl, create_session):
    mock_method.return_value("Data has been written out")
    mock_class.return_value.name = "df"

    schema = StructType([
        StructField("id", StringType(), nullable=False),
        StructField("short_id", IntegerType(), nullable=False)
    ])

    data = [("000001", 1), ("000002", 2)]

    sdf = create_session.createDataFrame(data, schema)

    mock_class().pipeline_read_data.return_value = sdf
    test_method = PipelineMethod("RSI_20200520_15141987", "method_a",
                                 "tests.test_methods.bd.big_data",
                                 [{"name": "df", "database": "test_db",
                                   "table": "test_table",
                                   "path": "dummy.json",
                                   "select": ["column_1", "column_2"],
                                   "where": [{"column": "run_id",
                                              "condition": "=", "value": "previous"}]}],
                                 {
                                     "location": "s3://dtrades-assets/workflows",
                                     "format": "parquet",
                                     "save_mode": "append",
                                     "partition_by": ["run_id"]
                                 },
                                 True,
                                 logger,
                                 {"param_1": "col_1", "param_2": "col_2",
                                  "param_3": "col_3"})

    test_method.run("test-crawler", create_session)


@patch('spp.engine.pipeline.crawl')
@patch('spp.engine.pipeline.write_data')
@patch('spp.engine.pipeline.DataAccess')
def test_aws_big_pipeline(mock_class, mock_method, mock_crawl, create_session):
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

    data_1 = [("201602", "test_name", 1000),
              ("201603", "not_test_name", 1000),
              ("201604", "test_name", 1000)]
    data_2 = [("201602", "test_name", True),
              ("201603", "not_test_name", False)]

    dfs = [create_session.createDataFrame(data, schema),
           create_session.createDataFrame(data_1, schema_1),
           create_session.createDataFrame(data_2, schema_2)]

    mock_method.return_value("Data has been written out")
    type(mock_class()).name = PropertyMock(side_effect=df_names)
    mock_class().pipeline_read_data.side_effect = dfs

    test_pipeline = Pipeline("Test", "000001", logger, True)

    test_pipeline.add_pipeline_methods("run_id", "method_a",
                                       "tests.test_methods.bd.big_data",
                                       [{"name": "df", "database": "test_db",
                                         "table": "test_table",
                                         "path": "dummy.json",
                                         "select": ["column_1", "column_2"],
                                         "where": [{"column": "column_1",
                                                    "condition": "=",
                                                    "value": 100}]}],
                                       {
                                           "location": "s3://dtrades-assets/workflows",
                                           "format": "parquet",
                                           "save_mode": "append",
                                           "partition_by": ["run_id"]
                                       }, True,
                                       {"param_1": "col_1", "param_2": "col_2",
                                        "param_3": "col_3"})
    test_pipeline.add_pipeline_methods("run_id", "method_b",
                                       "tests.test_methods.bd.big_data",
                                       [{"name": "df_1", "path": "dummy.json",
                                         "database": "test_db_1",
                                         "table": "test_table_1",
                                         "select": ["column_1", "column_2"],
                                         "where": [{"column": "column_1",
                                                    "condition": "=", "value": 100}]},
                                        {"name": "df_2", "path": "dummy2.json",
                                         "database": "test_db_2",
                                         "table": "test_table_2",
                                         "select": ["column_1", "column_2"],
                                         "where": [{"column": "column_2",
                                                    "condition": "<", "value": 500}]}],
                                       {
                                           "location": "s3://dtrades-assets/workflows",
                                           "format": "parquet",
                                           "save_mode": "append",
                                           "partition_by": ["run_id"]
                                       }, True,
                                       {"param_1": "reporting_date",
                                        "param_2": "entity_name"})
    test_pipeline.run(crawler_name="test-crawler")
