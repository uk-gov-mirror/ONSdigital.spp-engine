from awsglue.context import GlueContext
from spp.utils.logging import Logger

LOG = Logger(__name__).get()
def glue_sparkshell_handler(spark_Session):
    LOG.info("Creat GlueContext start ")
    glue_context = GlueContext(spark_Session.sparkContext)
    spark = glue_context.spark_session
    LOG.info("GlueContext has been created ")
    return spark
