from unittest.mock import patch

import pandas as pd
from es_aws_functions import general_functions

from spp.engine.data_access import DataAccess
from spp.utils.query import Query

logger = general_functions.get_logger(survey="rsi", module_name="SPP Engine - Write",
                                      environment="sandbox", run_id="1111.2222")


@patch('spp.engine.data_access.pandas_read')
@patch('spp.engine.data_access.spark_read')
def test_aws_big_pipeline_read_data_1(mock_spark_read, mock_pandas_read, create_session):
    data_access = DataAccess(
        name="df",
        query=Query(
            database='test_db',
            table='test_table',
            select=['col1', 'col2'],
            where='col1 = 100',
            run_id="fake_run_id"
        ),
        logger=logger
    )
    data_access.pipeline_read_data(create_session)
    assert mock_spark_read.called
    assert not mock_pandas_read.called


@patch('spp.engine.data_access.pandas_read')
@patch('spp.engine.data_access.spark_read')
def test_aws_big_pipeline_read_data_2(mock_spark_read, mock_pandas_read, create_session):
    data_access = DataAccess(
        name="df",
        query="s3://BUCKET_NAME/PATH-TO-INPUT-DATA/",
        logger=logger
    )
    data_access.pipeline_read_data(create_session)
    assert mock_spark_read.called
    assert not mock_pandas_read.called


@patch('spp.engine.data_access.pandas_read')
@patch('spp.engine.data_access.spark_read')
def test_aws_small_pipeline_read_data_1(mock_spark_read, mock_pandas_read):
    data_access = DataAccess(
        name="df",
        query="s3://BUCKET_NAME/PATH-TO-INPUT-DATA/",
        logger=logger
    )
    data_access.pipeline_read_data()
    assert not mock_spark_read.called
    assert mock_pandas_read.called


@patch('spp.engine.read.PandasAthenaReader.read_db')
@patch('spp.engine.data_access.spark_read')
def test_aws_small_pipeline_read_data_2(mock_spark_read, mock_pandas_athena):
    data_access = DataAccess(
        name="df",
        query=Query(
            database='test_db',
            table='test_table',
            select=['col1', 'col2'],
            where='col1 = 100',
            run_id="fake_run_id"
        ),
        logger=logger
    )
    data_access.pipeline_read_data()
    mock_pandas_athena.return_value = pd.DataFrame({"old_col": pd.Series([2])})
    assert not mock_spark_read.called
    assert mock_pandas_athena.called
