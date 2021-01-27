from spp.utils.query import Query
import importlib

current_module = "SPP Engine - Read"


class PandasReader:
    def read_db(self, query, **kwargs):
        """ To be implemented by child classes """
        raise NotImplementedError("Abstract method.")

    def read_file(self, path, **kwargs):
        """
        Reads a file from a file path into a Pandas DataFrame.
        Selects the Pandas read method from the file extension.
        :param kwargs: Other keyword arguments to pass to pd.read{format}()
        :param path: String representing file path
        :returns Pandas DataFrame:
        """
        return getattr(
            importlib.import_module("pandas"), "read_{}".format(_get_file_format(path))
        )(path, **kwargs)

    def __repr__(self):
        return "PandasReader"


class PandasAthenaReader(PandasReader):
    def read_db(self, query, **kwargs):
        """
        Reads an Athena table into a Pandas DataFrame using an SPP Query instance.
        :param query: Query instance
        :returns Pandas DataFrame:
        """
        import awswrangler as wr

        return wr.athena.read_sql_query(
            sql=str(query)[:-1], database=query.database, **kwargs
        )

    def __repr__(self):
        return "PandasAthenaReader"


def pandas_read(cursor, logger, reader=PandasReader(), **kwargs):
    """
    Reads data into a DataFrame using Pandas. If the cursor is an SPP Query,
    a database is queried and a
    PandasReader instance must be supplied which implements read_db(),
    otherwise the cursor string is treated like a file path.
    :param cursor: Query instance or file path String
    :param kwargs: Other keyword arguments to pass to pd.read_{format}()
    :param logger: SPP logger object to pass on to log functions
    :param reader: DB connection object that extends PandasReader
    :returns Pandas DataFrame:
    """
    # If cursor looks like query
    if isinstance(cursor, Query):
        logger.debug(f"Reading from database, query: {str(cursor)} reader: {reader}")
        if reader:
            return reader.read_db(cursor, **kwargs)
        else:
            raise Exception("Cursor is query-like, but no reader object given")

    # Otherwise, treat as file location
    else:
        logger.debug(f"Reading from file {cursor}")
        return reader.read_file(cursor, **kwargs)


def spark_read(spark, cursor, logger, **kwargs):
    """
    Reads data into a DataFrame using Spark. If the cursor is an SPP Query,
    the Spark metastore is used,
    otherwise the cursor is treated like a file path.
    :param cursor: Query instance or file path String
    :param kwargs: Other keyword arguments to pass to spark.read.load()
    :param logger: SPP logger object to pass to logger functions
    :param spark: Spark session
    :returns Spark DataFrame:
    """

    # If cursor looks like query
    if isinstance(cursor, Query):
        logger.debug(f"Reading from database, query: {str(cursor)[:-1]} reader: {spark}")
        return spark.sql(str(cursor)[:-1])

    # Otherwise, treat as file location
    else:
        logger.debug(f"Reading from file {cursor}")
        return spark.read.load(cursor, format=_get_file_format(cursor), **kwargs)


def _get_file_format(location):
    return location.split(".")[-1]
