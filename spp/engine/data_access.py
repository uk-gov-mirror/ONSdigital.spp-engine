from spp.utils.query import Query

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
    '''The purpose of this function is to set the run id in a way which
    means that you have no idea whether the input was correct.
    Of course if you don't pass a list for your partition column 
    (which is entirely ignored otherwise) the function does nothing'''
    if (df is not None) and (list_partition_column is not None) and is_spark:
        import pyspark.sql.functions as f
        columns = df.columns
        if 'run_id' in columns:
            df = df.drop('run_id').withColumn('run_id', f.lit(run_id))

        df = df.withColumn('run_id', f.lit(run_id))
    return df



def spark_write(df, data_target, counter, logger, **kwargs):
    """
    Writes a Spark DataFrame to a file.
    :param counter: Int used to modify file path name if there is already a file
    existing with the desired name
    :param data_target: Dictionary containing information on where to save the data
    :param df: Spark DataFrame
    :param logger:
    :param kwargs: Other keyword arguments to pass to df.write.save()
    """
    tmp_path = ''
    if isinstance(counter, int) & (counter >= 1):
        tmp_path = "/data" + str(counter)
    data_target['location'] = data_target['location'] + tmp_path
    write_spark_df_to_s3(df, data_target, logger=logger)
    logger.debug("Writing to file location: " + data_target['location'])


def write_spark_df_to_s3(df, data_target, logger):
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.dynamicframe import DynamicFrame

    logger.debug(f"Writing spark dataframe to {repr(data_target)}")
    glue_context = GlueContext(SparkContext.getOrCreate())
    dynamic_df_out = DynamicFrame.fromDF(df, glue_context, "dynamic_df_out")

    block_size = 128 * 1024 * 1024
    page_size = 1024 * 1024
    glue_context.write_dynamic_frame.from_options(
        frame=dynamic_df_out,
        connection_type="s3",
        connection_options={
            "path": data_target["location"],
            "partitionKeys": data_target["partition_by"],
        },
        format="glueparquet",
        format_options={
            "compression": "snappy",
            "blockSize": block_size,
            "pageSize": page_size,
        },
    )
    logger.debug("write complete")


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


