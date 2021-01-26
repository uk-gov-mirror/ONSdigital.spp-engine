from spp.engine.read import spark_read, pandas_read, PandasAthenaReader
from spp.engine.write import spark_write, pandas_write
from spp.utils.query import Query
import spp.engine.pipeline
from es_aws_functions import general_functions

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
        :param environment: Current running environment to pass to spp logger
        :param name: String
        :param query: spp.utils.query.Query
        :param run_id: Current run_id to pass to spp logger
        :param survey: Current running survey to pass to spp logger
        :return:
        """
        self.environment = environment
        self.run_id = run_id
        self.survey = survey
        self.logger = general_functions.get_logger(self.survey, current_module,
                                                   self.environment, self.run_id)
        self.logger.debug("Initializing DataAccess")
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
        self.logger.debug("DataAccess: read data using : {}".format(self.query))

        if spark is not None:
            self.logger.debug("DataAccess: read data into spark dataframe")
            return spark_read(environment=self.environment, run_id=self.run_id,
                              survey=self.survey, spark=spark, cursor=self.query)
        else:
            if (platform == spp.engine.pipeline.Platform.AWS.value) & \
                    (isinstance(self.query, Query)):
                self.logger.debug("DataAccess: read data into pandas dataframe")
                return pandas_read(cursor=self.query, environment=self.environment,
                                   run_id=self.run_id, survey=self.survey,
                                   reader=PandasAthenaReader())
            else:
                self.logger.debug("DataAccess: read data into pandas dataframe")
                return pandas_read(cursor=self.query, environment=self.environment,
                                   run_id=self.run_id, survey=self.survey)


def write_data(output, data_target, platform,
               environment, run_id, survey,
               spark=None, counter=0):
    """
    This method may be removed as further requirements
    determine whether this should be a generic function
    :param data_target: target location
    :param environment: Environment name for logger
    :param output: Dataframe
    :param platform: Platform
    :param run_id: Run_id name for logger
    :param spark: SparkSession
    :param survey: Survey name for logger
    :return:
    """
    logger = general_functions.get_logger(survey, current_module,
                                          environment, run_id)
    logger.debug("DataAccess: write data: ")
    if spark is not None:
        logger.debug("DataAccess: write spark dataframe")
        spark_write(df=output, data_target=data_target, counter=counter,
                    environment=environment, run_id=run_id, survey=survey)
        logger.debug("DataAccess: written spark dataframe successfully")
        return
    else:
        logger.debug("DataAccess: write pandas dataframe")
        pandas_write(df=output, data_target=data_target,
                     environment=environment, run_id=run_id, survey=survey)
        logger.debug("DataAccess: written pandas dataframe successfully")
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
