from enum import Enum
from spp.engine.data_access import write_data, \
    DataAccess, isPartitionColumnExists
from spp.utils.logging import Logger
import importlib
from spp.aws.glue_crawler import crawl

from spp.utils.query import Query

from es_aws_functions import aws_functions

LOG = Logger(__name__).get()


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
                 data_target, write, params=None):
        """
        Initialise the attributes of the class
        :param name: String
        :param module: String
        :param data_source: list of Dict[String, Dict]
        :param data_target: target location
        :param params: Dict[String, Any]
        """
        LOG.info("Initializing Method")
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
        LOG.info("Retrieving data")
        inputs = {data.name: data.pipeline_read_data(platform, spark)
                  for data in self.data_in}
        LOG.info("Data Retrieved")
        LOG.debug("Retrieved data : {}".format(inputs))
        LOG.info("Importing Module")
        try:
            module = importlib.import_module(self.module_name)
            LOG.debug("Imported {}".format(module))
        except Exception as e:
            LOG.exception("Cant import module {}".format(self.module_name))
            raise e
        LOG.info("Calling Method")
        try:
            LOG.debug("Calling Method: {} with parameters {} {}".format(self.method_name,
                                                                        inputs,
                                                                        self.params))
            outputs = getattr(module, self.method_name)(**inputs, **self.params)
        except TypeError as e:
            LOG.exception("Incorrect Parameters for method called.")
            raise e
        except ValueError as e:
            LOG.exception("Issue with getting parameter name.")
            raise e
        except Exception as e:
            LOG.exception("Issue calling method")
            raise e

        if self.write:
            LOG.info("Writing outputs")
            LOG.debug("Writing outputs: {}".format(outputs))
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
                        LOG.debug("Writing output: {}".format(output))
                else:
                    outputs = isPartitionColumnExists(outputs,
                                                      self.data_target['partition_by'],
                                                      str(self.run_id), is_spark)
                    write_data(output=outputs, data_target=self.data_target,
                               platform=platform, spark=spark)
                    LOG.debug("Writing output: {}".format(outputs))
                LOG.info("Finished writing outputs Method run complete")
            crawl(crawler_name=crawler_name)
        else:
            LOG.info("Returning outputs dataframe")
            return outputs


class Pipeline:
    """
    Wrapper to contain the pipeline methods and enable their callling
    """
    platform = None
    name = None
    methods = None
    spark = None
    run_id = None

    def __init__(self, name, run_id, platform=Platform.AWS, is_spark=False,
                 queue_url=None):
        """
        Initialises the attributes of the class.
        :param name:
        :param platform: Platform
        :param is_spark: Boolean
        :param queue_url: String or None if there is no queue to send status to
        """
        LOG.info("Initializing Pipeline")
        self.name = name
        self.platform = platform
        self.run_id = run_id
        if is_spark:
            LOG.info("Starting Spark Session for APP {}".format(name))
            from pyspark.sql import SparkSession
            self.spark = SparkSession.builder.appName(name).getOrCreate()
            # self. spark.conf.set("spark.sql.parquet.mergeSchema", "true")
        self.queue_url = queue_url
        self.methods = []

    def add_pipeline_methods(self, run_id, name, module, data_source,
                             data_target, write, params):
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
        :return:
        """
        LOG.info("Adding Method to Pipeline")
        LOG.debug(
            "Adding Method: {} , from Module {}, With parameters {}, "
            "retrieving data from {}, writing to {}.".format(
                name,
                module,
                params,
                data_source,
                str(data_target)))
        self.methods.append(PipelineMethod(run_id, name, module,
                                           data_source, data_target, write, params))

    def send_status(self, status, module_name):
        """
        Send a status message for the pipeline
        :param status: The status to send - String
        :param module_name: the name of the module to be reported - String
        :return:
        """
        if self.queue_url is None:
            return

        aws_functions.send_bpm_status(self.queue_url, status, module_name,
                                      self.run_id, survey='RSI')

    def run(self, platform, crawler_name):
        """
        Runs the methods of the pipeline
        :param platform: Platform
        :return:
        """
        LOG.info("Running Pipeline: {}".format(self.name))
        self.send_status("IN PROGRESS", self.name)
        for method in self.methods:
            LOG.info("Running Method: {}".format(method.method_name))
            self.send_status('IN PROGRESS', method.method_name)
            method.run(platform, crawler_name, self.spark)
            self.send_status('DONE', method.method_name)
            LOG.info("Method Finished: {}".format(method.method_name))

        self.send_status('DONE', self.name)


def construct_pipeline(config):
    LOG.debug("Constructing pipeline with name {}, platform {}, is_spark {}".format(
        config['name'], config['platform'], config['spark']
    ))
    pipeline = Pipeline(
        name=config['name'], run_id=config['run_id'],
        platform=config['platform'], is_spark=config['spark'],
        queue_url=config.get('queue_url')
    )

    for method in config['methods']:
        LOG.debug("Adding method with name {}, module {}, queries {}, params {}".format(
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
                                      params=method['params'][0]
                                      )

    return pipeline
