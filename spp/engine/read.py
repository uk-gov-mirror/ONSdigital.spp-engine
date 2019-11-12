import pandas as pd
from spp.engine.query import Query
import logging
import importlib


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class PandasReader:

    def read_db(self, cursor):
        raise NotImplementedError('Abstract method.')

    def read_file(self, cursor, **kwargs):
        return getattr(importlib.import_module('pandas'), f'read_{_get_file_format(cursor)}')(cursor, **kwargs)

    def __repr__(self):
        return 'PandasReader'


class PandasAthenaReader(PandasReader):

    def read_db(self, cursor):
        import awswrangler
        session = awswrangler.Session()
        return session.pandas.read_sql_athena(sql=cursor)

    def __repr__(self):
        return 'PandasAthenaReader'


def pandas_read(cursor, reader=PandasReader(), **kwargs):

    """
    Reads data into a DataFrame using Pandas. If the cursor string is query-like, a database is queried and a
    reader object must be supplied, otherwise the cursor string is treated like a file path.
    :param cursor: String representing query or file location
    :param reader: DB connection object that extends Pandas Reader
    :param kwargs: Other keyword arguments to pass to pd.read_{format}()
    :returns Pandas DataFrame:
    """
    # If cursor looks like query
    if isinstance(cursor, Query):
        _db_log(str(cursor)[:-1], reader)
        if reader:
            return reader.read_db(str(cursor)[:-1])
        else:
            raise Exception('Cursor is query-like, but no reader object given')

    # Otherwise, treat as file location
    else:
        _file_log(cursor)
        return reader.read_file(cursor, **kwargs)


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


def _get_file_format(location):
    return location.split('.')[-1]


def _db_log(cursor, connection):
    logger.info(f"Reading from database")
    logger.info(f"Query: {cursor}")
    logger.info(f"Connection: {connection}")


def _file_log(cursor):
    logger.info(f"Reading from file")
    logger.info(f"Location: {cursor}")
