from unittest.mock import PropertyMock, patch

from es_aws_functions import general_functions
from spp.engine.pipeline import Pipeline

logger = general_functions.get_logger(survey="rsi", module_name="SPP Engine - Write",
                                      environment="sandbox", run_id="1111.2222")
@patch('es_aws_functions.aws_functions.send_bpm_status')
@patch('spp.engine.pipeline.PipelineMethod')
def test_aws_big_pipeline(mock_class, mock_method):
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
    assert test_pipeline.run(crawler_name="test-crawler")
    assert mock_method.called
