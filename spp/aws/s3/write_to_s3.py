from es_aws_functions import general_functions

current_module = "SPP Engine - Write to S3"


def write_pandasDf_to_s3(df, data_target, environment, run_id, survey):
    # input_datafame,partition_cols,bucket_name=None, filepath=None, format=None):

    logger = general_functions.get_logger(survey, current_module,
                                          environment, run_id)

    logger.debug(("Pandas write to {}. Partition_by : "
                  + str(data_target['partition_by']) + ". Save mode : "
                  + str(data_target['save_mode'])).format(data_target['location']))

    import awswrangler as wr
    wr.s3.to_parquet(df=df, path=data_target['location'],
                     compression='snappy', dataset=True,
                     partition_cols=data_target['partition_by'],
                     mode=str(data_target['save_mode']).lower())

    # Approach 3 with awswrangler version 1.0.4 end

    logger.debug("Pandas write completed.")

    return


def write_sparkDf_to_s3(df, data_target, environment, run_id, survey):
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.dynamicframe import DynamicFrame

    logger = general_functions.get_logger(survey, current_module,
                                          environment, run_id)

    glueContext = GlueContext(SparkContext.getOrCreate())
    logger.debug('Inside write_sparkDf_to_s3 :: Created glueContext')
    dynamic_df_out = DynamicFrame.fromDF(df, glueContext, "dynamic_df_out")
    logger.debug('Inside write_sparkDf_to_s3 :: '
                'Convert spark df to dynamic df completed ... ')
    logger.debug('Inside write_sparkDf_to_s3 ::  '
                'partition :: string : ' + str(data_target['partition_by']))
    logger.debug('Inside write_sparkDf_to_s3 :: '
                'writing to location ' + data_target['location'])
    block_size = 128 * 1024 * 1024
    page_size = 1024 * 1024
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_df_out,
        connection_type="s3",
        connection_options={"path": data_target['location'],
                            "partitionKeys": data_target['partition_by']},
        format="glueparquet",
        format_options={"compression": "snappy",
                        "blockSize": block_size, "pageSize": page_size})
    logger.debug('Inside write_sparkDf_to_s3 :: completed.')
    return
