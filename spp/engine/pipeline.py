import importlib
import pyspark.sql
from es_aws_functions import aws_functions

current_module = "SPP-Engine - Pipeline"


class PipelineMethod:
    """
    Wrapper that contains the metadata for a pipeline method
    that will be called as part of a pipeline
    """

    def __init__(
        self,
        run_id,
        name,
        module,
        data_source,
        data_target,
        logger,
        params=None,
        provide_session=False,
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
        if params is not None:
            if not isinstance(params, dict):
                raise TypeError(f"params must be None or dict not {type(params)}")
            self.params = params

        else:
            self.params = {}

        self.run_id = run_id
        self.data_source = data_source
        self.data_target = data_target
        self.provide_session = provide_session

    def run(self, spark):
        """
        Will import the method and call it.  It will then write out
        the outputs
        :param spark: SparkSession builder
        :return:
        """
        if self.provide_session:
            self.params["spark"] = spark

        if self.data_source is not None:
            self.logger.debug("Retrieving data from %r", self.data_source)
            df = spark.table(self.data_source)
            df = df.filter(df.run_id == self.run_id)
            if df.count() == 0:
                raise RuntimeError(f"Found no rows for run id {self.run_id}")

        self.logger.debug(f"Importing module {self.module_name}")
        module = importlib.import_module(self.module_name)
        self.logger.debug(f"{self.method_name} params {repr(self.params)}")
        self.params["df"] = df
        output = getattr(module, self.method_name)(**self.params)

        if self.data_target is not None:
            # We need to transform our schema and hope that the datatypes
            # match

            output = output.select(spark.table(self.data_target).columns)
            output.write.insertInto(self.data_target["location"], overwrite=True)


class Pipeline:
    """
    Wrapper to contain the pipeline methods and enable their calling
    """

    def __init__(self, name, run_id, logger, bpm_queue_url=None):
        """
        Initialises the attributes of the class.
        :param bpm_queue_url: String or None if there is no queue to send status to
        :param name: Name of pipeline run
        :param run_id: Current run id
        :param logger: the logger to use
        """
        self.logger = logger
        self.name = name
        self.run_id = run_id
        self.logger.debug("Starting Spark Session for APP {}".format(name))
        self.spark = (
            pyspark.sql.SparkSession.builder.enableHiveSupport()
            .appName(name)
            .getOrCreate()
        )
        self.spark.sql("SET spark.sql.sources.partitionOverwriteMode=dynamic")
        self.spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
        self.bpm_queue_url = bpm_queue_url
        self.methods = []

    def add_pipeline_methods(
        self, run_id, name, module, data_source, data_target, params, provide_session
    ):
        """
        Adds a new method to the pipeline
        :param data_source: list of Dict[String, Dict]
        :param data_target: dictionary of string (currently just location)
        :param module: Method module name from config
        :param name: Method name from config
        :param params: Dict[String, Any]
        :param run_id: run_id - String
        :param provide_session: Whether to provide the session object - Boolean
        :param logger: logger to use
        :return:
        """

        self.methods.append(
            PipelineMethod(
                run_id,
                name,
                module,
                data_source,
                data_target,
                self.logger,
                params,
                provide_session,
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

    def run(self):
        """
        Runs the methods of the pipeline
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
                method.run(self.spark)
                self.send_status("DONE", method.method_name, current_step_num=step_num)
                self.logger.info("Method Finished: {}".format(method.method_name))

            self.send_status("DONE", self.name)
        except Exception:
            # The logger knows how to correctly extract the exception from
            # sys.exc_info
            self.logger.exception("Error running pipeline")
            self.send_status("ERROR", self.name)
            return False

        return True


def construct_pipeline(config, logger):
    logger.info(f"Constructing pipeline {config['name']}")
    pipeline = Pipeline(
        name=config["name"],
        run_id=config["run_id"],
        logger=logger,
        bpm_queue_url=config.get("bpm_queue_url"),
    )

    for method in config["methods"]:
        pipeline.add_pipeline_methods(
            run_id=config["run_id"],
            name=method["name"],
            module=method["module"],
            data_source=method.get("data_source"),
            data_target=method.get("data_target"),
            params=method.get("params"),
            provide_session=method.get("provide_session", False),
        )

    return pipeline
