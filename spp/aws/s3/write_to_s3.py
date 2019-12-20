
from spp.utils.logging import Logger

LOG = Logger(__name__).get()


def write_pandas_to_s3(df, data_target):
    # input_datafame,partition_cols,bucket_name=None, filepath=None, format=None):
    LOG.info("Pandas write to {}".format(data_target['location']))
    print('df dtypes')
    print(df.dtypes)

    ##Todo revisit with lates version of pyarrow
    ##Approach 1 start  : Note there is an open issue : https://issues.apache.org/jira/browse/ARROW-5156 on 19/12/2019
    # if data_target['format'] == 'parquet':
    #     out_buffer = BytesIO()
    #     df.to_parquet(out_buffer, partition_cols=data_target['partition_by'], index=False, engine='pyarrow')
    #
    #
    # # elif format == 'csv':
    # #     out_buffer = StringIO()
    # #     input_datafame.to_parquet(out_buffer,index=False)
    #
    # s3_client.put_object(Bucket=parts.netloc, Key=parts.path.lstrip('/'), Body=out_buffer.getvalue())
    ##Approach 1 end
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    import s3fs

    if data_target['format'] == 'parquet':
        ## Approach 2 start

        ##Todo revisit with lates version of pyarrow
        # To work around an Open pyarrow bug . Please refer more details on
        # https://issues.apache.org/jira/browse/ARROW-5379 . check on on 19/12/2019
        for col in df:
            if isinstance(df[col].dtype, pd.Int64Dtype):
                df[col] = df[col].astype('object')
        print(df.dtypes)

        fs = s3fs.S3FileSystem()
        table = pa.Table.from_pandas(df)
        pq.write_to_dataset(table, data_target['location'], filesystem=fs, partition_cols=data_target['partition_by'],
                            use_dictionary=True, compression='snappy', use_deprecated_int96_timestamps=True)
        ##Approach 2 end
    else:
        LOG.error("Currently Pandas write supports only parquet format... No support available : {}".format(
            data_target['format']))
        LOG.error("Skipping write data ....  ")
        return

    LOG.info("Pandas write completed ")

    return
