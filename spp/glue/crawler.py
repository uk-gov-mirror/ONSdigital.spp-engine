import boto3
import time


class Crawler:

    def crawl(self):

        client = boto3.client('glue')

        client.start_crawler(Name = 'dtrades-admin')

        while(client.get_crawler(Name = 'dtrades-admin')['Crawler']['State'] in ["RUNNING", "STOPPING"]):
              time.sleep(10)


