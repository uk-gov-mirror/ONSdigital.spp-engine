import boto3
import time
from es_aws_functions import general_functions

current_module = "SPP Engine - Glue Crawler"


def crawl(crawler_name, environment, run_id, survey):
    logger = general_functions.get_logger(survey, current_module,
                                          environment, run_id)
    logger.debug("crawler : {}".format(crawler_name)+" starts..")
    client = boto3.client('glue', region_name='eu-west-2')
    client.start_crawler(Name=crawler_name)
    while client.get_crawler(Name=crawler_name)['Crawler']['State'] in \
            ["RUNNING", "STOPPING"]:
        time.sleep(10)
    logger.debug("crawler : {}".format(crawler_name)+" completed")
