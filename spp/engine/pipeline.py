from enum import Enum
from typing import Iterable
from pyspark.sql import SparkSession
from engine.data_access import write_data, DataAccess

import importlib


class Platform(Enum):
    """
    Enum that indicates which platform the Pipeline will run on currently only AWS but planned are GCP and Azure
    """
    AWS = 1


class PipelineMethod:
    """
    Wrapper that contains the metadata for a pipeline method that will be called as part of a pipeline
    """

    package = None
    module_name = None
    method_name = None
    params = None
    data_in = None

    def __init__(self, name, module, queries, params=None):
        """
        Initialise the attributes of the class
        :param name: String
        :param module: String
        :param package: String
        :param queries: Dict[String, spp.utils.query.Query]
        :param params: Dict[String, Any]
        """
        self.method_name = name
        self.module_name = module
        self.params = params
        self.data_in = []
        for query_name, query in queries.items():
            self.data_in.append(DataAccess(query_name, query))

    def run(self, platform, spark=None):
        """
        Will import the method and call it.  It will then write out the outputs
        :param platform:
        :param spark:
        :return:
        """
        inputs = {data.name: data.read_data(platform, spark) for data in self.data_in}
        module = importlib.import_module(self.module_name)
        outputs = getattr(module, self.method_name)(**inputs, **self.params)
        if isinstance(outputs, Iterable):
            for output in outputs:
                write_data(output, platform, spark)
        else:
            write_data(outputs, platform, spark)


class Pipeline:
    """
    Wrapper to contain the pipeline methods and enable their callling
    """
    platform = None
    name = None
    methods = None
    spark = None

    def __init__(self, name, platform=Platform.AWS, is_spark=False):
        """
        Initialises the attributes of the class.
        :param name:
        :param platform: Platform
        :param is_spark: Boolean
        """
        self.name = name
        self.platform = platform
        if is_spark:
            self.spark = SparkSession.builder.appName(name).getOrCreate()
        self.methods = []

    def add_pipeline_methods(self, name, module, queries, params):
        """
        Adds a new method to the pipeline
        :param name: String
        :param module: String
        :param package: String
        :param queries: Dict[String, spp.utils.query.Query]
        :param params: Dict[String, Any]
        :return:
        """
        self.methods.append(PipelineMethod(name, module, queries, params))

    def run(self, platform):
        """
        Runs the methods of the pipeline
        :param platform: Platform
        :param spark: SparkSession
        :return:
        """
        for method in self.methods:
            method.run(platform, self.spark)
