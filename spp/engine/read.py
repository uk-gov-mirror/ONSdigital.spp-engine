from pyspark.sql import SparkSession
from .query import Query
import pandas as pd
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def read_db(connection, database, table, select=None, where=None):

    """
    Reads a DataFrame from a database connected to the Spark metastore or a connection object.
    :param connection: Spark session or DB connection object
    :param database: Database name
    :param table: Table name
    :param select: Column or [columns] to select
    :param where: Filter condition
    :returns Spark/Pandas DataFrame:
    """

    query = str(Query(database, table, select, where))
    logger.info(f"Reading from {database} database")
    logger.info(f"Query: {query}")
    logger.info(f"Connection: {str(connection)}")
    if isinstance(connection, SparkSession):
        return connection.sql(query)
    else:
        return pd.read_sql(query, connection)


def read_file(location, spark=None):

    """
    Reads a DataFrame from a file.
    :param location: File location
    :param spark: If not None, use Spark
    :returns Spark/Pandas DataFrame:
    """

    file_format = location.split('.')[-1]  # cvs, json, parquet...
    logger.info(f"Reading from file")
    logger.info(f"Location: {location}")
    if spark:
        return spark.read.load(location, format=format)
    else:
        import s3fs
        return getattr(__import__('pandas'), f'read_{file_format}')(location)
