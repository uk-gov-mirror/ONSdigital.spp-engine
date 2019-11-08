import pandas as pd
from spp.utils.query import Query
from spp.utils.logging import Logger
import importlib


LOG = Logger(__name__).get()


def spark_read(spark, cursor, **kwargs):

    """
    Reads data into a DataFrame using Spark. If the cursor is an SPP Query, the Spark metastore is used,
    otherwise the cursor is treated like a file path String.
    :param spark: Spark session
    :param cursor: Query object or file location String
    :param kwargs: Other keyword arguments to pass to spark.read.load()
    :returns Spark DataFrame:
    """

    # If cursor looks like query
    if isinstance(cursor, Query):
        _db_log(str(cursor)[:-1], spark)
        return spark.sql(str(cursor)[:-1])

    # Otherwise, treat as file location
    else:
        _file_log(cursor)
        return spark.read.load(cursor, format=_get_file_format(cursor), **kwargs)


def pandas_read(cursor, connection=None, **kwargs):

    """
    Reads data into a DataFrame using Pandas. If the cursor string is query-like, a database is queried and a
    connection object must be supplied, otherwise the cursor string is treated like a file path.
    :param connection: DB connection object
    :param cursor: String representing query or file location
    :param kwargs: Other keyword arguments to pass to pd.read_{format}()
    :returns Pandas DataFrame:
    """
    # If cursor looks like query
    if isinstance(cursor, Query):
        _db_log(str(cursor)[:-1], connection)
        if connection:
            return pd.read_sql(str(cursor)[:-1], connection)
        else:
            raise Exception('Cursor is query-like, but no connection object given')

    # Otherwise, treat as file location
    else:
        _file_log(cursor)
        return getattr(importlib.import_module('pandas'), f'read_{_get_file_format(cursor)}')(cursor, **kwargs)


def _get_file_format(location):
    return location.split('.')[-1]


def _db_log(cursor, connection):
    LOG.info(f"Reading from database")
    LOG.info(f"Query: {cursor}")
    LOG.info(f"Connection: {connection}")


def _file_log(cursor):
    LOG.info(f"Reading from file")
    LOG.info(f"Location: {cursor}")
