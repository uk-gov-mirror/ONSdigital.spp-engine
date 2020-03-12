from spp.utils.logging import Logger
import logging

LOG = Logger(__name__).get()


def write_pandasDf_to_s3(df, data_target):
    # input_datafame,partition_cols,bucket_name=None, filepath=None, format=None):
    LOG.info("Pandas write to {}".format(data_target['location']))

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


def write_sparkDf_to_s3(df, data_target):
    LOG.info('Inside spark write :: No dynamic dataframe  ... ')
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.dynamicframe import DynamicFrame

    glueContext = GlueContext(SparkContext.getOrCreate())
    dynamic_df_out = DynamicFrame.fromDF(df, glueContext, "dynamic_df_out")
    LOG.info('Inside write_sparkDf_to_s3 :: writing to location ... ' + data_target['location'])
    block_size = 128 * 1024 * 1024
    page_size = 1024 * 1024
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_df_out,
        connection_type="s3",
        connection_options={"path": data_target['location'], "partitionKeys": data_target['partition_by']},
        format="glueparquet",
        format_options={"compression": "snappy",
                        "blockSize": block_size, "pageSize": page_size})
    LOG.info('Inside write_sparkDf_to_s3 :: completed ... ')
    return
