from enum import Enum
from typing import Iterable
from pyspark.sql import SparkSession
from engine.data_access import write_data

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

    def __init__(self, name, module, package, queries, params=None):
        """
        Initialise the attributes of the class
        :param name: String
        :param module: String
        :param package: String
        :param queries: list[spp.utils.query.Query]
        :param params: Dict[String, Any]
        """
        self.method_name = name
        self.module_name = module
        self.package = package
        self.params = params
        self.data_in = []
        for query in queries:
            self.data_in.append(DataAccess(query))

    def run(self, platform, spark=None):
        """
        Will import the method and call it.  It will then write out the outputs
        :param platform:
        :param spark:
        :return:
        """
        inputs = [data.get_data(platform, spark) for data in self.data_in]
        module = importlib.import_module(self.module_name, self.package)
        outputs = getattr(module, self.method_name)(*inputs, **self.params)
        if isinstance(outputs, Iterable):
            for output in outputs:
                write_data(output)
        else:
            write_data(outputs)



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

    def add_pipeline_methods(self, name, module, package, queries, params):
        """
        Adds a new method to the pipeline
        :param name: String
        :param module: String
        :param package: String
        :param queries: List[spp.utils.query.Query]
        :param params: Dict[string, Any]
        :return:
        """
        self.methods.append(PipelineMethod(name, module, package, queries, params))

    def run(self, platform, spark):
        """
        Runs the methods of the pipeline
        :param platform: Platform
        :param spark: SparkSession
        :return:
        """
        for method in self.methods:
            method.run(platform, spark)
