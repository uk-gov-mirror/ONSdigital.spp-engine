from unittest.mock import patch, PropertyMock
from spp.engine.pipeline import PipelineMethod, Platform, Pipeline
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, BooleanType
import pandas as pd
import spp.engine.pipeline as p_module
from spp.engine.data_access import write_data, DataAccess
from spp.engine.read import spark_read,pandas_read
from spp.utils.query import Query



# @patch('spp.engine.read.spark_read')
# def test_aws_small_pipeline_read_data(mock_spark_read,create_session):
#     data_access = DataAccess(name = "df",query = Query('test_db', 'test_table', ['col1', 'col2'], 'col1 = 100'))
#     schema = StructType([
#         StructField("id", StringType(), nullable=False),
#         StructField("short_id", IntegerType(), nullable=False)
#     ])
#
#     data = [("000001", 1), ("000002", 2)]
#
#     sdf = create_session.createDataFrame(data, schema)
#
#     #mock_class().re.return_value = sdf
#     data_access.pipeline_read_data(p_module.Platform.AWS,create_session)
#     assertTrue(mock_spark_read.called)
#
