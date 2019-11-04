from enum import Enum
from typing import Iterable
from pyspark.sql import SparkSession

import importlib


class Platform(Enum):
    """
    Enum that indicates which platform the Pipeline will run on currently only AWS but planned are GCP and Azure
    """
    AWS = 1


class DataAccess:
    """
    Wrapper that calls the differing Data Access methods depending on the platform that the pipeline is running on and
    whether it is utilising Apache Spark or is a pure python project
    """

    query = None

    def __int__(self, query):
        """
        Takes in the Query object that is used to access the data
        :param query: spp.utils.query.Query
        :return:
        """
        self.query = query

    def get_data(self, platform, spark=None):
        """
        Will call the specific data retrieval method depending on the Platform and whether it is spark or not
        using the query supplies when instantiation the class.
        :param platform: Platform
        :param spark: SparkSession
        :return:
        """
        if spark is None:
            # TODO import the correct data access class and call get data to return in
            return
        else:
            # TODO spark version of the above
            return


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
                self._write_data(output)
        else:
            self._write_data(outputs)

    def _write_data(self, output, platform, spark=None):
        """
        This method may be removed as further requirements determine whether this should be a generic function
        :param output: Dataframe
        :param platform: Platform
        :param spark: SparkSession
        :return:
        """
        if spark is None:
            # TODO write outputs
            return
        else:
            # TODO Spark Version
            return


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

    def create_spark_session(self, name):


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