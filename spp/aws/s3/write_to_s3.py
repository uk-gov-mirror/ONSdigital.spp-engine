from es_aws_functions import general_functions

current_module = "SPP Engine - Write to S3"


def write_pandasDf_to_s3(df, data_target, environment, run_id, survey):
    # input_datafame,partition_cols,bucket_name=None, filepath=None, format=None):
    try:
        logger = general_functions.get_logger(survey, current_module,
                                              environment, run_id)
    except Exception as e:
        raise Exception("{}:Exception raised: {}".format(current_module, e))
    logger.info("Pandas write to {}".format(data_target['location']))
    logger.info('Pandas write_pandasDf_to_s3 :: '
                'partition_by : ' + str(data_target['partition_by']))
    logger.info('Pandas write_pandasDf_to_s3 :: '
                'save mode : ' + str(data_target['save_mode']))
    import awswrangler as wr
    wr.s3.to_parquet(df=df, path=data_target['location'],
                     compression='snappy', dataset=True,
                     partition_cols=data_target['partition_by'],
                     mode=str(data_target['save_mode']).lower())

    # Approach 3 with awswrangler version 1.0.4 end

    logger.info("Pandas write completed ")

    return


def write_sparkDf_to_s3(df, data_target, environment, run_id, survey):
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.dynamicframe import DynamicFrame
    try:
        logger = general_functions.get_logger(survey, current_module,
                                              environment, run_id)
    except Exception as e:
        raise Exception("{}:Exception raised: {}".format(current_module, e))
    glueContext = GlueContext(SparkContext.getOrCreate())
    logger.info('Inside write_sparkDf_to_s3 :: Created glueContext ... ')
    dynamic_df_out = DynamicFrame.fromDF(df, glueContext, "dynamic_df_out")
    logger.info('Inside write_sparkDf_to_s3 :: '
                'Convert spark df to dynamic df completed ... ')
    logger.info('Inside write_sparkDf_to_s3 ::  '
                'partition :: string : .... ' + str(data_target['partition_by']))
    logger.info('Inside write_sparkDf_to_s3 :: '
                'writing to location ... ' + data_target['location'])
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
    logger.info('Inside write_sparkDf_to_s3 :: completed ... ')
    return
