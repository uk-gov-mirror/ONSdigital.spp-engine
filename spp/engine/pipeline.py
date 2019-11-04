from enum import Enum


class Platform(Enum):
    AWS = 1


class DataAccess:

    query = None
    platform = None
    spark = None

    def __int__(self, query, platform, spark):
        self.query = query
        self.platform = platform
        self.spark
        return


class PipelineMethod:

    package = ''
    method_name = ''
    params_list = ''

    def __init__(self):
        return



class Pipeline:

    platform = Platform.AWS

    def __init__(self):
        return






