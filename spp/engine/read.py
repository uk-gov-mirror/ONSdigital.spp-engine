import importlib

from spp.utils.query import Query

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
