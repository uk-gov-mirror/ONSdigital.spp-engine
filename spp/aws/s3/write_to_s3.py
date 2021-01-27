current_module = "SPP Engine - Write to S3"


def write_pandas_df_to_s3(df, data_target, logger):
    # input_datafame,partition_cols,bucket_name=None, filepath=None, format=None):

    logger.debug(f"Pandas write data target {repr(data_target)}")

    import awswrangler as wr

    wr.s3.to_parquet(
        df=df,
        path=data_target["location"],
        compression="snappy",
        dataset=True,
        partition_cols=data_target["partition_by"],
        mode=str(data_target["save_mode"]).lower(),
    )
    logger.debug("Pandas write completed.")


def write_spark_df_to_s3(df, data_target, logger):
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.dynamicframe import DynamicFrame

    logger.debug(f"Writing spark dataframe to {repr(data_target)}")
    glue_context = GlueContext(SparkContext.getOrCreate())
    dynamic_df_out = DynamicFrame.fromDF(df, glue_context, "dynamic_df_out")

    block_size = 128 * 1024 * 1024
    page_size = 1024 * 1024
    glue_context.write_dynamic_frame.from_options(
        frame=dynamic_df_out,
        connection_type="s3",
        connection_options={
            "path": data_target["location"],
            "partitionKeys": data_target["partition_by"],
        },
        format="glueparquet",
        format_options={
            "compression": "snappy",
            "blockSize": block_size,
            "pageSize": page_size,
        },
    )
    logger.debug("write complete")
