from spp.utils.query import Query
from es_aws_functions import aws_functions, general_functions
import importlib

current_module = "SPP Engine - Read"


class PandasReader:
    def read_db(self, query, **kwargs):
        """ To be implemented by child classes """
        raise NotImplementedError('Abstract method.')

    def read_file(self, path, **kwargs):
        """
        Reads a file from a file path into a Pandas DataFrame.
        Selects the Pandas read method from the file extension.
        :param kwargs: Other keyword arguments to pass to pd.read{format}()
        :param path: String representing file path
        :returns Pandas DataFrame:
        """
        return getattr(importlib.import_module('pandas'),
                       "read_{}".format(_get_file_format(path)))(path, **kwargs)

    def __repr__(self):
        return 'PandasReader'


class PandasAthenaReader(PandasReader):
    def read_db(self, query, **kwargs):
        """
        Reads an Athena table into a Pandas DataFrame using an SPP Query instance.
        :param query: Query instance
        :returns Pandas DataFrame:
        """
        import awswrangler as wr

        return wr.athena.read_sql_query(sql=str(query)[:-1],
                                        database=query.database, **kwargs)

    def __repr__(self):
        return 'PandasAthenaReader'


def pandas_read(cursor, environment, run_id,
                survey, reader=PandasReader(), **kwargs):
    """
    Reads data into a DataFrame using Pandas. If the cursor is an SPP Query,
    a database is queried and a
    PandasReader instance must be supplied which implements read_db(),
    otherwise the cursor string is treated like a file path.
    :param cursor: Query instance or file path String
    :param environment: Current running environment to pass to spp logger
    :param kwargs: Other keyword arguments to pass to pd.read_{format}()
    :param reader: DB connection object that extends PandasReader
    :param run_id: Current run_id to pass to spp logger
    :param survey: Current running survey to pass to spp logger
    :returns Pandas DataFrame:
    """
    # If cursor looks like query
    if isinstance(cursor, Query):
        _db_log(str(cursor), reader, environment,
                run_id, survey)
        if reader:
            return reader.read_db(cursor, **kwargs)
        else:
            raise Exception('Cursor is query-like, but no reader object given')

    # Otherwise, treat as file location
    else:
        _file_log(cursor, environment, run_id, survey)
        return reader.read_file(cursor, **kwargs)


def spark_read(spark, cursor, environment,
               run_id, survey, **kwargs):
    """
    Reads data into a DataFrame using Spark. If the cursor is an SPP Query,
    the Spark metastore is used,
    otherwise the cursor is treated like a file path.
    :param cursor: Query instance or file path String
    :param environment: Current running environment to pass to spp logger
    :param kwargs: Other keyword arguments to pass to spark.read.load()
    :param run_id: Current run_id to pass to spp logger
    :param spark: Spark session
    :param survey: Current running survey to pass to spp logger
    :returns Spark DataFrame:
    """

    # If cursor looks like query
    if isinstance(cursor, Query):
        _db_log(str(cursor)[:-1], spark, environment,
                run_id, survey)
        return spark.sql(str(cursor)[:-1])

    # Otherwise, treat as file location
    else:
        _file_log(cursor, environment, run_id, survey)
        return spark.read.load(cursor, format=_get_file_format(cursor), **kwargs)


def _get_file_format(location):
    return location.split('.')[-1]


def _db_log(query, reader, environment,
            run_id, survey):
    try:
        logger = general_functions.get_logger(survey, current_module,
                                              environment, run_id)
    except Exception as e:
        raise Exception("{}:Exception raised: {}".format(current_module, e))
    logger.info("Reading from database")
    logger.info(f"Query: {query}")
    logger.info(f"Reader: {reader}")


def _file_log(path, environment, run_id,
              survey):
    try:
        logger = general_functions.get_logger(survey, current_module,
                                              environment, run_id)
    except Exception as e:
        raise Exception("{}:Exception raised: {}".format(current_module, e))
    logger.info("Reading from file")
    logger.info(f"Location: {path}")
