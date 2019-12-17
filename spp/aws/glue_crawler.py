import boto3
import time


def crawl(crawler):

    client = boto3.client('glue')

    client.start_crawler(Name = crawler)

    while client.get_crawler(Name = crawler)['Crawler']['State'] in ["RUNNING", "STOPPING"]:
        time.sleep(10)
