import importlib
import time

import boto3
from es_aws_functions import aws_functions

from pyspark.sql import SparkSession

current_module = "SPP-Engine - Pipeline"

class PipelineMethod:
    """
    Wrapper that contains the metadata for a pipeline method
    that will be called as part of a pipeline
    """

    def __init__(
        self, run_id, name, module, data_source, data_target, write, logger, params=None
    ):
        """
        Initialise the attributes of the class
        :param data_source: list of Dict[String, Dict]
        :param data_target: target location
        :param module: Method module name
        :param name: Method name
        :param params: Dict[String, Any]
        :param run_id: Current run_id for query
        :param write: Boolean of whether to write results to location
        :param logger: logger to use
        """
        self.logger = logger
        self.method_name = name
        self.module_name = module
        self.write = write
        self.params = params
        self.data_in = []
        self.run_id = run_id
        self.data_target = data_target

        for da in data_source:
            query = None
            if da.get("database") is not None:
                query = Query(
                    database=da["database"],
                    table=da["table"],
                    select=da["select"],
                    where=da["where"],
                    run_id=self.run_id,
                )

            else:
                query = da["path"]

            self.data_in.append(DataAccess(da["name"], query, logger))

    def run(self, crawler_name, spark=None):
        """
        Will import the method and call it.  It will then write out the outputs
        :param crawler_name: Name of the glue crawler
        :param spark: SparkSession builder
        :return:
        """
        self.logger.debug("Retrieving data")
        inputs = {data.name: data.pipeline_read_data(spark) for data in self.data_in}

        self.logger.debug(f"Importing module {self.module_name}")
        module = importlib.import_module(self.module_name)
        self.logger.debug(f"{self.method_name} params {repr(self.params)}")
        outputs = getattr(module, self.method_name)(**inputs, **self.params)

        if self.write:
            if self.data_target is not None:
                    outputs = set_run_id(outputs, str(self.run_id))
                    write_data(
                        output=outputs,
                        data_target=self.data_target,
                        logger=self.logger,
                        spark=spark
                    )

            crawl(crawler_name=crawler_name, logger=self.logger)

        else:
            return outputs


class Pipeline:
    """
    Wrapper to contain the pipeline methods and enable their calling
    """

    def __init__(self, name, run_id, logger, is_spark=False, bpm_queue_url=None):
        """
        Initialises the attributes of the class.
        :param bpm_queue_url: String or None if there is no queue to send status to
        :param is_spark: Boolean
        :param name: Name of pipeline run
        :param run_id: Current run id
        :param logger: the logger to use
        """
        self.logger = logger
        self.name = name
        self.run_id = run_id
        if is_spark:
            self.logger.debug("Starting Spark Session for APP {}".format(name))
            self.spark = SparkSession.builder.appName(name).getOrCreate()

        else:
            self.spark = None

        self.bpm_queue_url = bpm_queue_url
        self.methods = []

    def add_pipeline_methods(
        self, run_id, name, module, data_source, data_target, write, params
    ):
        """
        Adds a new method to the pipeline
        :param data_source: list of Dict[String, Dict]
        :param data_target: dictionary of string related to write.such as location,
        format and partition column.
        :param module: Method module name from config
        :param name: Method name from config
        :param params: Dict[String, Any]
        :param run_id: run_id - String
        :param write: Whether or not to write the data - Boolean
        :param logger: logger to use
        :return:
        """
        self.logger.debug(
            "Adding Method: {} , from Module {}, With parameters {}, "
            "retrieving data from {}, writing to {}.".format(
                name, module, params, data_source, repr(data_target)
            )
        )
        self.methods.append(
            PipelineMethod(
                run_id,
                name,
                module,
                data_source,
                data_target,
                write,
                self.logger,
                params
            )
        )

    def send_status(self, status, module_name, current_step_num=None):
        """
        Send a status message for the pipeline
        :param current_step_num: the number of the step in the pipeline - Int or None
        :param module_name: the name of the module to be reported - String
        :param status: The status to send - String
        :return:
        """
        if self.bpm_queue_url is None:
            return

        aws_functions.send_bpm_status(
            self.bpm_queue_url,
            module_name,
            status,
            self.run_id,
            survey="RSI",
            current_step_num=current_step_num,
            total_steps=len(self.methods),
        )

    def run(self, crawler_name):
        """
        Runs the methods of the pipeline
        :param crawler_name: Name of glue crawler to run for each method
        :param survey: Current running survey to pass to spp logger
        :return:
        """
        self.send_status("IN PROGRESS", self.name)
        try:
            for method_num, method in enumerate(self.methods):
                self.logger.info("Running Method: {}".format(method.method_name))
                # method_num is 0-indexed but we probably want step numbers
                # to be 1-indexed
                step_num = method_num + 1
                self.send_status(
                    "IN PROGRESS", method.method_name, current_step_num=step_num
                )
                method.run(crawler_name, self.spark)
                self.send_status("DONE", method.method_name, current_step_num=step_num)
                self.logger.info("Method Finished: {}".format(method.method_name))

            self.send_status("DONE", self.name)
        except Exception:
            # The logger knows how to correctly extract the exception from
            # sys.exc_info
            self.logger.exception("Error running pipeline")
            self.send_status("ERROR", self.name)


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

def set_run_id(df, run_id):
    '''
    The purpose of this function is to set the run id in a way which
    means that you have no idea whether the input was correct.
    '''
    if df is not None:
        import pyspark.sql.functions as f
        columns = df.columns
        if 'run_id' in columns:
            df = df.drop('run_id')

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


def construct_pipeline(config, logger):
    logger.info(
        "Constructing pipeline with name {}, using spark {}".format(
            config["name"], config["spark"]
        )
    )
    pipeline = Pipeline(
        name=config["name"],
        run_id=config["run_id"],
        logger=logger,
        is_spark=config["spark"],
        bpm_queue_url=config.get("bpm_queue_url")
    )

    for method in config["methods"]:
        if method["write"]:
            write_data_to = method["data_write"][0]
        else:
            write_data_to = None
        pipeline.add_pipeline_methods(
            run_id=config["run_id"],
            name=method["name"],
            module=method["module"],
            data_source=method["data_access"],
            data_target=write_data_to,
            write=method["write"],
            params=method["params"][0]
        )

    return pipeline


def crawl(crawler_name, logger):
    logger.debug("crawler : {}".format(crawler_name) + " starts..")
    client = boto3.client("glue", region_name="eu-west-2")
    client.start_crawler(Name=crawler_name)
    while client.get_crawler(Name=crawler_name)["Crawler"]["State"] in [
        "RUNNING",
        "STOPPING"
    ]:
        time.sleep(10)
    logger.debug("crawler : {}".format(crawler_name) + " completed")
current_module = "SPP Engine - Query"


class Query:
    """ Class to create a SQL Query string from the input parameters. """

    def __init__(self, database, table, select=None,
                 where=None, run_id=None):
        """ Constructor for the Query class takes
            database and table optional for select which can be string or list
            Optional where expects a map of format
            {"column_name": {"condition": value, "value": value}}
        """
        self.database = database
        self.table = table
        self.select = select
        self.where = where
        self.run_id = run_id
        self._formulate_query(self.database, self.table, self.select, self.where)

    def __str__(self):
        return self.query

    def _handle_select(self, select):
        """ Method to generate select statements if None default to select all: '*'. """
        if select is None:
            sel = "*"
        elif isinstance(select, list):
            sel = ", ".join(select)
        else:
            sel = select
        return sel

    def _handle_where(self, where_conds):
        """ Method to generate where statements form map"""
        if where_conds is not None and \
                isinstance(where_conds, list) \
                and len(where_conds) > 0:
            clause_list = []
            condition_str = ''

            for whr in where_conds:
                # Todo remove handle it on lambda itself.
                # currently config(json) string escape not
                # working as expected in lamda/stepfunction.
                # This is a work around
                if whr["column"] == 'run_id':
                    # Replace word 'previous' with
                    # a run_id which got generated in steprunner lambda
                    if whr["value"] == 'previous':
                        whr["value"] = self.run_id
                    whr["value"] = "'" + whr["value"] + "'"
                clause_list.append("{} {} {}".format(whr["column"],
                                                     whr["condition"],
                                                     str(whr["value"])))
                condition_str = " AND ".join(clause_list).rstrip(" AND ")
            return condition_str

        else:
            return None

    def _formulate_query(self, database, table, select, where):
        """ Method to create the query string and set the query value
        this is called by the constructor """
        tmp = "SELECT {} FROM {}.{}".format(self._handle_select(select), database, table)
        where_clause = self._handle_where(where)
        if where_clause is not None:
            tmp += " WHERE {}".format(where_clause)
        self.query = tmp + ";"
