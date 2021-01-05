from spp.engine.read import spark_read, pandas_read, PandasAthenaReader
from spp.engine.write import spark_write, pandas_write
from spp.utils.query import Query
import spp.engine.pipeline
from es_aws_functions import aws_functions, general_functions

current_module = "DataAccess"


class DataAccess:
    """
    Wrapper that calls the differing Data Access methods depending on
    the platform that the pipeline is running on and
    whether it is utilising Apache Spark or is a pure python project
    """

    query = None
    name = None

    def __init__(self, name, query, survey,
                 environment, run_id):
        """
        Takes in the Query object that is used to access the data
        :param name: String
        :param query: spp.utils.query.Query
        :return:
        """
        self.environment = environment
        self.run_id = run_id
        self.survey = survey
        try:
            self.logger = general_functions.get_logger(self.survey, current_module,
                                                       self.environment, self.run_id)
        except Exception as e:
            raise Exception("{}:Exception raised: {}".format(current_module, e))
        self.logger.info("Initializing DataAccess")
        self.query = query
        self.name = name

    def pipeline_read_data(self, platform, spark=None):
        """
        Will call the specific data retrieval method depending
        on the Platform and whether it is spark or not
        using the query supplies when instantiation the class.
        :param platform: Platform
        :param spark: SparkSession
        :return:
        """
        self.logger.info("DataAccess: read data:")
        self.logger.info("DataAccess: read data using : {}".format(self.query))

        if spark is not None:
            self.logger.info("DataAccess: read data into spark dataframe")
            return spark_read(environment=self.environment, run_id=self.run_id,
                              survey=self.survey, spark=spark, cursor=self.query)
        else:
            if (platform == spp.engine.pipeline.Platform.AWS.value) & \
                    (isinstance(self.query, Query)):
                self.logger.info("DataAccess: read data into pandas dataframe")
                return pandas_read(cursor=self.query, environment=self.environment,
                                   run_id=self.run_id, survey=self.survey, reader=PandasAthenaReader())
            else:
                self.logger.info("DataAccess: read data into pandas dataframe")
                return pandas_read(cursor=self.query, environment=self.environment,
                                   run_id=self.run_id, survey=self.survey)


def write_data(output, data_target, platform,
               environment, run_id, survey,
               spark=None, counter=0):
    """
    This method may be removed as further requirements
    determine whether this should be a generic function
    :param output: Dataframe
    :param data_target: target location
    :param platform: Platform
    :param environment: Environment name for logger
    :param run_id: Run_id name for logger
    :param survey: Survey name for logger
    :param spark: SparkSession
    :return:
    """
    try:
        logger = general_functions.get_logger(survey, current_module,
                                              environment, run_id)
    except Exception as e:
        raise Exception("{}:Exception raised: {}".format(current_module, e))
    logger.info("DataAccess: write data: ")
    if spark is not None:
        logger.info("DataAccess: write spark dataframe")
        spark_write(df=output, data_target=data_target, counter=counter)
        logger.info("DataAccess: written spark dataframe successfully")
        return
    else:
        logger.info("DataAccess: write pandas dataframe")
        pandas_write(df=output, data_target=data_target)
        logger.info("DataAccess: written pandas dataframe successfully")
        return


def isPartitionColumnExists(df, list_partition_column, run_id, is_spark,
                            environment, survey):
    try:
        logger = general_functions.get_logger(survey, current_module,
                                              environment, run_id)
    except Exception as e:
        raise Exception("{}:Exception raised: {}".format(current_module, e))
    logger.info('isPartitionColumnExists :: start..')
    if (df is not None) and (list_partition_column is not None) and is_spark:
        import pyspark.sql.functions as f
        columns = df.columns
        if 'run_id' not in columns:
            logger.info('inside spark run_id not exist')
            df = df.withColumn('run_id', f.lit(run_id))
        elif 'run_id' in columns:
            logger.info('inside spark run_id exist')
            df = df.drop('run_id').withColumn('run_id', f.lit(run_id))
    elif (df is not None) and (list_partition_column is not None) and not is_spark:
        columns = list(df.columns)
        if 'run_id' not in columns:
            logger.info('inside pandas run_id not exist')
            df['run_id'] = run_id
        elif 'run_id' in columns:
            logger.info('inside pandas run_id exist')
            df = df.drop(columns=['run_id'])
            df['run_id'] = run_id
    logger.info('isPartitionColumnExists :: end..')
    return df
