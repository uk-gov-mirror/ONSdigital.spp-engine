from enum import Enum
from spp.engine.data_access import write_data, DataAccess, isPartitionColumnExists
import importlib
from spp.aws.glue_crawler import crawl

from spp.utils.query import Query

from es_aws_functions import aws_functions, general_functions

current_module = "SPP-Engine - Pipeline"


class Platform(Enum):
    """
    Enum that indicates which platform the Pipeline
    will run on currently only AWS but planned are GCP and Azure
    """

    AWS = "AWS"


class PipelineMethod:
    """
    Wrapper that contains the metadata for a pipeline method
    that will be called as part of a pipeline
    """

    module_name = None
    method_name = None
    params = None
    data_in = None

    def __init__(
        self,
        run_id,
        name,
        module,
        data_source,
        data_target,
        write,
        environment,
        survey,
        params=None,
    ):
        """
        Initialise the attributes of the class
        :param data_source: list of Dict[String, Dict]
        :param data_target: target location
        :param environment: Current running environment to pass to logger
        :param module: Method module name
        :param name: Method name
        :param params: Dict[String, Any]
        :param run_id: Current run_id to pass on to spp logger
        :param survey: Current running survey to pass to logger
        :param write: Boolean of whether to write results to location
        """
        self.logger = general_functions.get_logger(
            survey, current_module, environment, run_id
        )
        self.method_name = name
        self.module_name = module
        self.write = write
        self.params = params
        self.data_in = []
        self.run_id = run_id
        self.data_target = data_target
        self.__populateDataAccess(data_source, survey, environment, run_id)

    def __populateDataAccess(self, data_source, survey, environment, run_id):
        da_key = []
        da_value = []
        for da in data_source:
            da_key.append(da["name"])
            tmp_sql = None
            if ("database" in da) and (da["database"] is not None):
                tmp_sql = Query(
                    database=da["database"],
                    table=da["table"],
                    environment=environment,
                    survey=survey,
                    select=da["select"],
                    where=da["where"],
                    run_id=self.run_id,
                )
            tmp_path = da["path"]
            da_value.append({"sql": tmp_sql, "path": tmp_path})
        data_source_tmp = dict(zip(da_key, da_value))
        for d_name, d_info in data_source_tmp.items():
            name = d_name
            query = None
            for key in d_info:
                if query is None:
                    query = d_info[key]
                elif key == "sql":
                    query = d_info[key]
            self.data_in.append(DataAccess(name, query, survey, environment, run_id))

    def run(self, platform, crawler_name, survey, environment, run_id, spark=None):
        """
        Will import the method and call it.  It will then write out the outputs
        :param crawler_name: Name of the glue crawler
        :param environment: Current running survey to be passed to spp logger
        :param platform: The platform the code is running on, most likely AWS
        :param run_id: Current run_id to be passed to spp logger
        :param spark: SparkSession builder
        :param survey: Current running survey to be passed to spp logger
        :return:
        """
        self.logger.debug("Retrieving data")
        inputs = {
            data.name: data.pipeline_read_data(platform, spark) for data in self.data_in
        }

        self.logger.debug(f"Importing module {self.module_name}")
        module = importlib.import_module(self.module_name)
        self.logger.debug(f"{self.method_name} params {repr(self.params)}")
        outputs = getattr(module, self.method_name)(**inputs, **self.params)

        if self.write:
            if self.data_target is not None:
                is_spark = True if spark is not None else False
                if isinstance(outputs, list) or isinstance(outputs, tuple):
                    for count, output in enumerate(outputs, start=1):
                        # (output, data_target, platform, spark=None,counter=None):
                        output = isPartitionColumnExists(
                            output,
                            self.data_target["partition_by"],
                            str(self.run_id),
                            is_spark,
                            environment,
                            survey,
                        )
                        write_data(
                            output=output,
                            data_target=self.data_target,
                            platform=platform,
                            environment=environment,
                            run_id=run_id,
                            survey=survey,
                            spark=spark,
                            counter=count,
                        )

                else:
                    outputs = isPartitionColumnExists(
                        outputs,
                        self.data_target["partition_by"],
                        str(self.run_id),
                        is_spark,
                        environment,
                        survey,
                    )
                    write_data(
                        output=outputs,
                        data_target=self.data_target,
                        platform=platform,
                        environment=environment,
                        run_id=run_id,
                        survey=survey,
                        spark=spark,
                    )

            crawl(
                crawler_name=crawler_name,
                environment=environment,
                run_id=run_id,
                survey=survey,
            )
        else:
            return outputs


class Pipeline:
    """
    Wrapper to contain the pipeline methods and enable their calling
    """

    platform = None
    name = None
    methods = None
    spark = None
    run_id = None

    def __init__(
        self,
        name,
        run_id,
        environment,
        survey,
        platform=Platform.AWS,
        is_spark=False,
        bpm_queue_url=None,
    ):
        """
        Initialises the attributes of the class.
        :param bpm_queue_url: String or None if there is no queue to send status to
        :param environment: Current running environment to be passed to spp logger
        :param is_spark: Boolean
        :param name: Name of pipeline run
        :param platform: Platform
        :param run_id: Current run_id to be passed to logger
        :param survey: Current running survey to be passed to spp logger
        """
        self.logger = general_functions.get_logger(
            survey, current_module, environment, run_id
        )
        self.logger.info("Initializing Pipeline")
        self.name = name
        self.platform = platform
        self.run_id = run_id
        if is_spark:
            self.logger.debug("Starting Spark Session for APP {}".format(name))
            from pyspark.sql import SparkSession

            self.spark = SparkSession.builder.appName(name).getOrCreate()
            # self. spark.conf.set("spark.sql.parquet.mergeSchema", "true")
        self.bpm_queue_url = bpm_queue_url
        self.methods = []

    def add_pipeline_methods(
        self,
        run_id,
        name,
        module,
        data_source,
        data_target,
        write,
        params,
        environment,
        survey,
    ):
        """
        Adds a new method to the pipeline
        :param data_source: list of Dict[String, Dict]
        :param data_target: dictionary of string related to write.such as location,
        format and partition column.
        :param environment: Current running environment to pass to spp logger
        :param module: Method module name from config
        :param name: Method name from config
        :param params: Dict[String, Any]
        :param run_id: run_id - String
        :param survey: Current running survey to pass to spp logger
        :param write: Whether or not to write the data - Boolean
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
                environment,
                survey,
                params,
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

    def run(self, platform, crawler_name, survey, environment, run_id):
        """
        Runs the methods of the pipeline
        :param crawler_name: Name of glue crawler to run for each method
        :param environment: Current running environment to pass to spp logger
        :param platform: Platform
        :param run_id: Current run_id to pass to spp logger
        :param survey: Current running survey to pass to spp logger
        :return:
        """
        self.logger.info("Running Pipeline: {}".format(self.name))
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
                method.run(
                    platform, crawler_name, survey, environment, run_id, self.spark
                )
                self.send_status("DONE", method.method_name, current_step_num=step_num)
                self.logger.info("Method Finished: {}".format(method.method_name))

            self.send_status("DONE", self.name)
        except Exception:
            # The logger knows how to correctly extract the exception from
            # sys.exc_info
            self.logger.exception("Error running pipeline")
            self.send_status("ERROR", self.name)

def construct_pipeline(config, survey):

    logger = general_functions.get_logger(
        survey, current_module, config["environment"], config["run_id"]
    )

    logger.info(
        "Constructing pipeline with name {}, platform {}, using spark {}".format(
            config["name"], config["platform"], config["spark"]
        )
    )
    pipeline = Pipeline(
        name=config["name"],
        run_id=config["run_id"],
        platform=config["platform"],
        is_spark=config["spark"],
        bpm_queue_url=config.get("bpm_queue_url"),
        environment=config["environment"],
        survey=survey,
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
            params=method["params"][0],
            environment=config["environment"],
            survey=survey,
        )

    return pipeline
