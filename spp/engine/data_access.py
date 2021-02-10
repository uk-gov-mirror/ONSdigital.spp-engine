

class DataAccess:
    """
    Wrapper that calls the spark_read method
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
        Call spark_read
        :param spark: SparkSession
        :return:
        """
        self.logger.debug("DataAccess: Read data using: {}".format(self.query))

        if spark is not None:
            self.logger.debug("DataAccess: Read data into spark dataframe")
            return spark_read(logger=self.logger, spark=spark, cursor=self.query)

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

def set_run_id(df, list_partition_column, run_id, is_spark):
    if (df is not None) and (list_partition_column is not None) and is_spark:
        import pyspark.sql.functions as f
        columns = df.columns
        if 'run_id' in columns:
            df = df.drop('run_id').withColumn('run_id', f.lit(run_id))

        df = df.withColumn('run_id', f.lit(run_id))
    return df
