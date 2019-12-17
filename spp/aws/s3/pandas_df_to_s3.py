import boto3
from io import BytesIO,StringIO
from urllib.parse import urlparse

def write_to_s3(s3_client,df,data_target):
    #input_datafame,partition_cols,bucket_name=None, filepath=None, format=None):
    print('Inside write_to_s3 .... start')
    print(str(data_target))
    parts = urlparse(url=data_target['location'], allow_fragments=False)
    print(str(parts))
    if data_target['format'] == 'parquet':
        out_buffer = BytesIO()
        df.to_parquet(out_buffer,partition_cols= data_target['partition_by'] ,index=False)

    # elif format == 'csv':
    #     out_buffer = StringIO()
    #     input_datafame.to_parquet(out_buffer,index=False)

    s3_client.put_object(Bucket=parts.netloc, Key=parts.path, Body=out_buffer.getvalue())
    return