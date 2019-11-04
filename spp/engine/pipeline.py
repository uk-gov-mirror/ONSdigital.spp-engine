from enum import Enum
from typing import Iterable

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

        :param name:
        :param module:
        :param package:
        :param queries:
        :param params:
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

    platform = None
    is_spark = None
    name = None
    methods = None

    def __init__(self, name, platform=Platform.AWS, is_spark=False):
        """

        :param name:
        :param platform:
        :param is_spark:
        """
        self.name = name
        self.platform = platform
        self.is_spark = is_spark
        self.methods = []

    def set_pipeline_methods(self, name, module, package, queries, params):
        """

        :param name:
        :param module:
        :param package:
        :param queries:
        :param params:
        :return:
        """
        self.methods.append(PipelineMethod(name, module, package, queries, params))

    def run(self, platform, spark):
        """

        :param platform:
        :param spark:
        :return:
        """
        for method in self.methods:
            method.run(platform, spark)