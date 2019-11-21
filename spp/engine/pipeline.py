from enum import Enum
from typing import Iterable
from spp.engine.data_access import write_data, DataAccess
from spp.utils.logging import Logger
import importlib

from spp.utils.query import Query

LOG = Logger(__name__).get()


class Platform(Enum):
    """
    Enum that indicates which platform the Pipeline will run on currently only AWS but planned are GCP and Azure
    """
    AWS = "AWS"


class PipelineMethod:
    """
    Wrapper that contains the metadata for a pipeline method that will be called as part of a pipeline
    """

    module_name = None
    method_name = None
    params = None
    data_in = None

    def __init__(self, name, module, data_source,data_target, params=None):
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
        self.params = params
        self.data_in = []
        self.data_target = data_target
        da_key =[]
        da_value = []
        #def __init__(self, database, table, select=None, where=None):
        for da in data_source:
            da_key.append(da['name'])
            tmp_sql = None
            if ('database' in da) and (da['database'] is not None):
                tmp_sql = Query(database = da['database'],table = da['table'],select = da['select'],where=da['where'])
            tmp_path = da['path']
            da_value.append({'sql':tmp_sql,'path':tmp_path})
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

    def run(self, platform, spark=None):
        """
        Will import the method and call it.  It will then write out the outputs
        :param platform:
        :param spark:
        :return:
        """
        LOG.info("Retrieving data")
        inputs = {data.name: data.pipeline_read_data(platform, spark) for data in self.data_in}
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
            LOG.debug("Calling Method: {} with parameters {} {}".format(self.method_name, inputs, self.params))
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
        LOG.info("Writing outputs")
        LOG.debug("Writing outputs: {}".format(outputs))
        if isinstance(outputs, Iterable):
            for count,output in enumerate(outputs,start=1):
                write_data(output,self.data_target+"/data"+str(count), platform, spark)
                LOG.debug("Writing output: {}".format(output))
        else:
            write_data(outputs,self.data_target, platform, spark)
            LOG.debug("Writing output: {}".format(outputs))
        LOG.info("Finished writing outputs Method run complete")


class Pipeline:
    """
    Wrapper to contain the pipeline methods and enable their callling
    """
    platform = None
    name = None
    methods = None
    spark = None
    run_id =None
    def __init__(self, name,run_id, platform=Platform.AWS, is_spark=False):
        """
        Initialises the attributes of the class.
        :param name:
        :param platform: Platform
        :param is_spark: Boolean
        """
        LOG.info("Initializing Pipeline")
        self.name = name
        self.platform = platform
        self.run_id = run_id
        if is_spark:
            LOG.info("Starting Spark Session for APP {}".format(name))
            from pyspark.sql import SparkSession
            self.spark = SparkSession.builder.appName(name).getOrCreate()
        self.methods = []

    def add_pipeline_methods(self, name, module, data_source,data_target_prefix, params):
        """
        Adds a new method to the pipeline
        :param name: String
        :param module: String
        :param data_source: list of Dict[String, Dict]
        :param data_target_prefix: target location prefix
        :param params: Dict[String, Any]
        :return:
        """
        part_of_path = ''
        file_separator = '/'

        if self.spark is not None:
            part_of_path = '/admin/'
        else:
            part_of_path = '/survey/'

        data_target_path = data_target_prefix+part_of_path+name+file_separator+self.run_id
        LOG.info("Adding Method to Pipeline")
        LOG.debug("Adding Method: {} , from Module {}, With parameters {}, retrieving data from {}, writing to {}.".format(name,
                                                                                                            module,
                                                                                                            params,
                                                                                                            data_source,
                                                                                                            data_target_path))
        self.methods.append(PipelineMethod(name, module, data_source,data_target_path, params))

    def run(self, platform):
        """
        Runs the methods of the pipeline
        :param platform: Platform
        :return:
        """
        LOG.info("Running Pipeline: {}".format(self.name))
        for method in self.methods:
            LOG.info("Running Method: {}".format(method.method_name))
            method.run(platform, self.spark)
            LOG.info("Method Finished: {}".format(method.method_name))