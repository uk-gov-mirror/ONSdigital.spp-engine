from enum import Enum
from spp.engine.data_access import write_data, \
    DataAccess, isPartitionColumnExists
from spp.utils.logging import Logger
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

    def __init__(self, run_id, name, module, data_source,
                 data_target, write, environment,
                 survey, params=None):
        """
        Initialise the attributes of the class
        :param name: String
        :param module: String
        :param data_source: list of Dict[String, Dict]
        :param data_target: target location
        :param params: Dict[String, Any]
        """
        try:
            self.logger = general_functions.get_logger(survey, current_module,
                                                       environment, run_id)
        except Exception as e:
            raise Exception("{}:Exception raised: {}".format(current_module, e))
        self.logger.info("Initializing Method")
        self.method_name = name
        self.module_name = module
        self.write = write
        self.params = params
        self.data_in = []
        self.run_id = run_id
        self.data_target = data_target
        self.__populateDataAccess(data_source)

    def __populateDataAccess(self, data_source):
        da_key = []
        da_value = []
        for da in data_source:
            da_key.append(da['name'])
            tmp_sql = None
            if ('database' in da) and (da['database'] is not None):
                tmp_sql = Query(database=da['database'],
                                table=da['table'],
                                select=da['select'],
                                where=da['where'],
                                run_id=self.run_id)
            tmp_path = da['path']
            da_value.append({'sql': tmp_sql, 'path': tmp_path})
        data_source_tmp = dict(zip(da_key, da_value))
        for d_name, d_info in data_source_tmp.items():
            name = d_name
            query = None
            for key in d_info:
                if query is None:
                    query = d_info[key]
                elif key == "sql":
                    query = d_info[key]
            self.data_in.append(DataAccess(name, query))

    def run(self, platform, crawler_name, spark=None):
        """
        Will import the method and call it.  It will then write out the outputs
        :param platform:
        :param crawler_name: Name of the glue crawler
        :param spark:
        :return:
        """
        self.logger.info("Retrieving data")
        inputs = {data.name: data.pipeline_read_data(platform, spark)
                  for data in self.data_in}
        self.logger.info("Data Retrieved")
        self.logger.debug("Retrieved data : {}".format(inputs))
        self.logger.info("Importing Module")
        try:
            module = importlib.import_module(self.module_name)
            self.logger.debug("Imported {}".format(module))
        except Exception as e:
            self.logger.error("Cant import module {}".format(self.module_name))
            raise e
        self.logger.info("Calling Method")
        try:
            self.logger.debug("Calling Method: {} with parameters {} {}".format(self.method_name,
                                                                                inputs,
                                                                                self.params))
            outputs = getattr(module, self.method_name)(**inputs, **self.params)
        except TypeError as e:
            self.logger.error("Incorrect Parameters for method called.")
            raise e
        except ValueError as e:
            self.logger.error("Issue with getting parameter name.")
            raise e
        except Exception as e:
            self.logger.error("Issue calling method")
            raise e

        if self.write:
            self.logger.info("Writing outputs")
            self.logger.debug("Writing outputs: {}".format(outputs))
            if self.data_target is not None:
                is_spark = True if spark is not None else False
                if isinstance(outputs, list) or isinstance(outputs, tuple):
                    for count, output in enumerate(outputs, start=1):
                        # (output, data_target, platform, spark=None,counter=None):
                        output = isPartitionColumnExists(output,
                                                         self.data_target[
                                                             'partition_by'],
                                                         str(self.run_id), is_spark)
                        write_data(output=output, data_target=self.data_target,
                                   platform=platform, spark=spark,
                                   counter=count)
                        self.logger.debug("Writing output: {}".format(output))
                else:
                    outputs = isPartitionColumnExists(outputs,
                                                      self.data_target['partition_by'],
                                                      str(self.run_id), is_spark)
                    write_data(output=outputs, data_target=self.data_target,
                               platform=platform, spark=spark)
                    self.logger.debug("Writing output: {}".format(outputs))
                self.logger.info("Finished writing outputs Method run complete")
            crawl(crawler_name=crawler_name)
        else:
            self.logger.info("Returning outputs dataframe")
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

    def __init__(self, name, run_id,
                 environment, survey,
                 platform=Platform.AWS,
                 is_spark=False, bpm_queue_url=None):
        """
        Initialises the attributes of the class.
        :param name:
        :param platform: Platform
        :param is_spark: Boolean
        :param bpm_queue_url: String or None if there is no queue to send status to
        """
        try:
            self.logger = general_functions.get_logger(survey, current_module,
                                                  environment, run_id)
        except Exception as e:
            raise Exception("{}:Exception raised: {}".format(current_module, e))
        self.logger.info("Initializing Pipeline")
        self.name = name
        self.platform = platform
        self.run_id = run_id
        if is_spark:
            self.logger.info("Starting Spark Session for APP {}".format(name))
            from pyspark.sql import SparkSession
            self.spark = SparkSession.builder.appName(name).getOrCreate()
            # self. spark.conf.set("spark.sql.parquet.mergeSchema", "true")
        self.bpm_queue_url = bpm_queue_url
        self.methods = []

    def add_pipeline_methods(self, run_id, name, module, data_source,
                             data_target, write, params, environment,
                             survey):
        """
        Adds a new method to the pipeline
        :param run_id: run_id - String
        :param name: String
        :param module: String
        :param data_source: list of Dict[String, Dict]
        :param data_target: dictionary of string related to write.such as location,
        format and partition column.
        :param write: Whether or not to write the data - Boolean
        :param params: Dict[String, Any]
        :param environment: String
        :param survey: String
        :return:
        """
        self.logger.info("Adding Method to Pipeline")
        self.logger.debug(
            "Adding Method: {} , from Module {}, With parameters {}, "
            "retrieving data from {}, writing to {}.".format(
                name,
                module,
                params,
                data_source,
                str(data_target)))
        self.methods.append(PipelineMethod(run_id, name, module,
                                           data_source, data_target, write,
                                           environment, survey, params))

    def send_status(self, status, module_name, current_step_num=None):
        """
        Send a status message for the pipeline
        :param status: The status to send - String
        :param module_name: the name of the module to be reported - String
        :param current_step_num: the number of the step in the pipeline - Int or None
        :return:
        """
        if self.bpm_queue_url is None:
            return

        aws_functions.send_bpm_status(self.bpm_queue_url, module_name, status,
                                      self.run_id, survey='RSI',
                                      current_step_num=current_step_num,
                                      total_steps=len(self.methods))

    def run(self, platform, crawler_name):
        """
        Runs the methods of the pipeline
        :param platform: Platform
        :return:
        """
        self.logger.info("Running Pipeline: {}".format(self.name))
        self.send_status("IN PROGRESS", self.name)
        for method_num, method in enumerate(self.methods):
            self.logger.info("Running Method: {}".format(method.method_name))
            # method_num is 0-indexed but we probably want step numbers
            # to be 1-indexed
            step_num = method_num+1
            self.send_status('IN PROGRESS', method.method_name, current_step_num=step_num)
            method.run(platform, crawler_name, self.spark)
            self.send_status('DONE', method.method_name, current_step_num=step_num)
            self.logger.info("Method Finished: {}".format(method.method_name))

        self.send_status('DONE', self.name)


def construct_pipeline(config, survey):

    try:
        logger = general_functions.get_logger(survey, current_module,
                                              config['environment'], config['run_id'])
    except Exception as e:
        raise Exception("{}:Exception raised: {}".format(current_module, e))

    logger.info("Constructing pipeline with name {}, platform {}, is_spark {}".format(
        config['name'], config['platform'], config['spark']
    ))
    pipeline = Pipeline(
        name=config['name'], run_id=config['run_id'],
        platform=config['platform'], is_spark=config['spark'],
        bpm_queue_url=config.get('bpm_queue_url'),
        environment=config["environment"], survey=survey
    )

    for method in config['methods']:
        logger.debug("Adding method with name {}, module {}, queries {}, params {}".format(
            method['name'], method['module'], method['data_access'], method['params']
        ))
        if method['write']:
            write_data_to = method['data_write'][0]
        else:
            write_data_to = None
        pipeline.add_pipeline_methods(run_id=config['run_id'],
                                      name=method['name'], module=method['module'],
                                      data_source=method['data_access'],
                                      data_target=write_data_to, write=method['write'],
                                      params=method['params'][0], environment=config["environment"],
                                      survey=survey
                                      )

    return pipeline
