from spp.engine.read import PandasAthenaReader, pandas_read, spark_read
from spp.engine.write import pandas_write, spark_write


class DataAccess:
    """
    Wrapper that calls the differing Data Access methods depending on
    the *** that the pipeline is running on and
    whether it is utilising Apache Spark or is a pure python project
    """

    def __init__(self, name, query, logger):
        """
        Takes in the Query object that is used to access the data
        :param name: String
        :param query: spp.utils.query.Query
        :param run_id: Current run_id to pass to spp logger
        :param logger: logger to use
        :return:
        """
        self.logger = logger
        self.logger.debug("Initializing DataAccess")
        self.query = query
        self.name = name

    def pipeline_read_data(self, spark=None):
        """
        Will call the specific data retrieval method depending
        on the *** and whether it is spark or not
        using the query supplies when instantiation the class.
        :param spark: SparkSession
        :return:
        """
        self.logger.debug("DataAccess: Read data using: {}".format(self.query))

        if spark is not None:
            self.logger.debug("DataAccess: Read data into spark dataframe")
            return spark_read(logger=self.logger, spark=spark, cursor=self.query)
        else:
            self.logger.debug("DataAccess: Read data into pandas dataframe")
            return pandas_read(cursor=self.query, logger=self.logger,
                               reader=PandasAthenaReader())


def write_data(output, data_target, logger,
               spark=None, counter=0):
    """
    This method may be removed as further requirements
    determine whether this should be a generic function
    :param data_target: target location
    :param logger: Logger object
    :param output: Dataframe
    :param spark: SparkSession
    :return:
    """
    logger.debug("DataAccess: Write data:")
    if spark is not None:
        logger.debug("DataAccess: Write spark dataframe")
        spark_write(df=output, data_target=data_target, counter=counter,
                    logger=logger)
        logger.debug("DataAccess: Written spark dataframe successfully")
        return
    else:
        logger.debug("DataAccess: Write pandas dataframe")
        pandas_write(df=output, data_target=data_target, logger=logger)
        logger.debug("DataAccess: Written pandas dataframe successfully")
        return


def set_run_id(df, list_partition_column, run_id, is_spark):
    if (df is not None) and (list_partition_column is not None) and is_spark:
        import pyspark.sql.functions as f
        columns = df.columns
        if 'run_id' not in columns:
            df = df.withColumn('run_id', f.lit(run_id))
        elif 'run_id' in columns:
            df = df.drop('run_id').withColumn('run_id', f.lit(run_id))
    elif (df is not None) and (list_partition_column is not None) and not is_spark:
        columns = list(df.columns)
        if 'run_id' not in columns:
            df['run_id'] = run_id
        elif 'run_id' in columns:
            df = df.drop(columns=['run_id'])
            df['run_id'] = run_id
    return df
