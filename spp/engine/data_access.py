from spp.engine.read import spark_read, pandas_read,PandasAthenaReader
from spp.engine.write import spark_write,pandas_write
from spp.utils.query import Query
import spp.engine.pipeline
from spp.utils.logging import Logger

LOG = Logger(__name__).get()

class DataAccess:
    """
    Wrapper that calls the differing Data Access methods depending on the platform that the pipeline is running on and
    whether it is utilising Apache Spark or is a pure python project
    """

    query = None
    name = None

    def __init__(self, name, query):
        """
        Takes in the Query object that is used to access the data
        :param name: String
        :param query: spp.utils.query.Query
        :return:
        """
        self.query = query
        self.name = name

    def pipeline_read_data(self, platform, spark=None):
        """
        Will call the specific data retrieval method depending on the Platform and whether it is spark or not
        using the query supplies when instantiation the class.
        :param platform: Platform
        :param spark: SparkSession
        :return:
        """
        LOG.info("Initializing Method")
        if spark is not None:
             return spark_read(spark=spark, cursor=self.query)
        else:
            if (platform ==  spp.engine.pipeline.Platform.AWS.value) & (isinstance(self.query, Query)):
                return pandas_read(cursor = self.query,reader=PandasAthenaReader())
            else:
                return pandas_read(cursor = self.query)

def write_data(output,data_target, platform, spark=None):
        """
        This method may be removed as further requirements determine whether this should be a generic function
        :param output: Dataframe
        :param data_target: target location
        :param platform: Platform
        :param spark: SparkSession
        :return:
        """
        if spark is not None:
            spark_write(df=output,location = data_target)
            return
        else:
            pandas_write(df = output, location = data_target)
            return
