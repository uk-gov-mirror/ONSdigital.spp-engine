from spp.utils.logging import Logger

LOG = Logger(__name__).get()


def write_pandasDf_to_s3(df, data_target):
    # input_datafame,partition_cols,bucket_name=None, filepath=None, format=None):
    LOG.info("Pandas write to {}".format(data_target['location']))
    LOG.info('Pandas write_pandasDf_to_s3 :: partition_by : ' + str(data_target['partition_by']))
    LOG.info('Pandas write_pandasDf_to_s3 :: save mode : ' + str(data_target['save_mode']))
    import awswrangler as wr
    wr.s3.to_parquet(df=df, path=data_target['location'], compression='snappy', dataset=True,
                     partition_cols=data_target['partition_by'], mode= str(data_target['save_mode']).lower())

    # Approach 3 with awswrangler version 1.0.4 end

    LOG.info("Pandas write completed ")

    return


def write_sparkDf_to_s3(df, data_target):
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.dynamicframe import DynamicFrame
    glueContext = GlueContext(SparkContext.getOrCreate())
    LOG.info('Inside write_sparkDf_to_s3 :: Created glueContext ... ')
    dynamic_df_out = DynamicFrame.fromDF(df, glueContext, "dynamic_df_out")
    LOG.info('Inside write_sparkDf_to_s3 :: Convert spark df to dynamic df completed ... ')
    LOG.info('Inside write_sparkDf_to_s3 ::  partition :: string : .... ' + str(data_target['partition_by']))
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
