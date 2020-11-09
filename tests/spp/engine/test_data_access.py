from unittest.mock import patch
import spp.engine.pipeline
from spp.engine.data_access import DataAccess
from spp.utils.query import Query
import pandas as pd


@patch('spp.engine.data_access.pandas_read')
@patch('spp.engine.data_access.spark_read')
def test_aws_big_pipeline_read_data_1(mock_spark_read, mock_pandas_read, create_session):
    data_access = DataAccess(name="df",
                             query=Query('test_db',
                                         'test_table',
                                         ['col1', 'col2'],
                                         'col1 = 100'))
    data_access.pipeline_read_data(spp.engine.pipeline.Platform.AWS, create_session)
    assert mock_spark_read.called
    assert not mock_pandas_read.called


@patch('spp.engine.data_access.pandas_read')
@patch('spp.engine.data_access.spark_read')
def test_aws_big_pipeline_read_data_2(mock_spark_read, mock_pandas_read, create_session):
    data_access = DataAccess(name="df", query="s3://BUCKET_NAME/PATH-TO-INPUT-DATA/")
    data_access.pipeline_read_data(spp.engine.pipeline.Platform.AWS, create_session)
    assert mock_spark_read.called
    assert not mock_pandas_read.called


@patch('spp.engine.data_access.pandas_read')
@patch('spp.engine.data_access.spark_read')
def test_aws_small_pipeline_read_data_1(mock_spark_read, mock_pandas_read):
    data_access = DataAccess(name="df", query="s3://BUCKET_NAME/PATH-TO-INPUT-DATA/")
    data_access.pipeline_read_data(spp.engine.pipeline.Platform.AWS)
    assert not mock_spark_read.called
    assert mock_pandas_read.called


@patch('spp.engine.read.PandasAthenaReader.read_db')
@patch('spp.engine.data_access.spark_read')
def test_aws_small_pipeline_read_data_2(mock_spark_read, mock_pandas_athena):
    data_access = DataAccess(name="df",
                             query=Query('test_db',
                                         'test_table',
                                         ['col1', 'col2'],
                                         'col1 = 100'))
    data_access.pipeline_read_data(spp.engine.pipeline.Platform.AWS.value)
    mock_pandas_athena.return_value = pd.DataFrame({"old_col": pd.Series([2])})
    assert not mock_spark_read.called
    assert mock_pandas_athena.called
