from spp.utils.query import Query
import logging
import importlib


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class PandasReader:

    def read_db(self, query, **kwargs):
        """ To be implemented by child classes """
        raise NotImplementedError('Abstract method.')

    def read_file(self, path, **kwargs):
        """
        Reads a file from a file path into a Pandas DataFrame. Selects the Pandas read method from the file extension.
        :param path: String representing file path
        :param kwargs: Other keyword arguments to pass to pd.read{format}()
        :returns Pandas DataFrame:
        """
        return getattr(importlib.import_module('pandas'), f'read_{_get_file_format(path)}')(path, **kwargs)

    def __repr__(self):
        return 'PandasReader'


class PandasAthenaReader(PandasReader):

    def read_db(self, query, **kwargs):
        """
        Reads an Athena table into a Pandas DataFrame using an SPP Query instance.
        :param query: Query instance
        :returns Pandas DataFrame:
        """
        import awswrangler
        session = awswrangler.Session()
        return session.pandas.read_sql_athena(sql=str(query)[:-1], **kwargs)

    def __repr__(self):
        return 'PandasAthenaReader'


def pandas_read(cursor, reader=PandasReader(), **kwargs):

    """
    Reads data into a DataFrame using Pandas. If the cursor is an SPP Query, a database is queried and a
    PandasReader instance must be supplied which implements read_db(), otherwise the cursor string is treated like a
    file path.
    :param cursor: Query instance or file path String
    :param reader: DB connection object that extends PandasReader
    :param kwargs: Other keyword arguments to pass to pd.read_{format}()
    :returns Pandas DataFrame:
    """
    # If cursor looks like query
    if isinstance(cursor, Query):
        _db_log(str(cursor), reader)
        if reader:
            return reader.read_db(cursor, **kwargs)
        else:
            raise Exception('Cursor is query-like, but no reader object given')

    # Otherwise, treat as file location
    else:
        _file_log(cursor)
        return reader.read_file(cursor, **kwargs)


def spark_read(spark, cursor, **kwargs):

    """
    Reads data into a DataFrame using Spark. If the cursor is an SPP Query, the Spark metastore is used,
    otherwise the cursor is treated like a file path.
    :param spark: Spark session
    :param cursor: Query instance or file path String
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


def _db_log(query, reader):
    logger.info(f"Reading from database")
    logger.info(f"Query: {query}")
    logger.info(f"Reader: {reader}")


def _file_log(path):
    logger.info(f"Reading from file")
    logger.info(f"Location: {path}")
