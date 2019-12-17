def write_to_s3(s3_client, input_datafame,bucket_name=None, filepath=None, format=None):

    if format == 'parquet':
        out_buffer = BytesIO()
        input_datafame.to_parquet(out_buffer,partition_cols=  ,index=False)

    elif format == 'csv':
        out_buffer = StringIO()
        input_datafame.to_parquet(out_buffer,index=False)

    s3_client.put_object(Bucket=bucket_name, Key=filepath, Body=out_buffer.getvalue())