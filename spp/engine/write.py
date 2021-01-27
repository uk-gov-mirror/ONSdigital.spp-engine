from spp.aws.s3.write_to_s3 import write_pandas_df_to_s3, write_spark_df_to_s3

current_module = "SPP Engine - Write"


def spark_write(df, data_target, counter, logger, **kwargs):
    """
    Writes a Spark DataFrame to a file.
    :param counter: Int used to modify file path name if there is already a file
    existing with the desired name
    :param data_target: Dictionary containing information on where to save the data
    :param df: Spark DataFrame
    :param logger:
    :param kwargs: Other keyword arguments to pass to df.write.save()
    """
    tmp_path = ''
    if isinstance(counter, int) & (counter >= 1):
        tmp_path = "/data" + str(counter)
    data_target['location'] = data_target['location'] + tmp_path
    write_spark_df_to_s3(df, data_target, logger=logger)
    logger.debug("Writing to file location: " + data_target['location'])


def pandas_write(df, data_target, logger, **kwargs):
    """
    Writes a Pandas DataFrame to a file.
    :param data_target: Dictionary containing information on where to save the data
    :param df: Pandas DataFrame
    :param logger:
    :param kwargs: Other keyword arguments to pass to df.to_{format}()
    """
    write_pandas_df_to_s3(df, data_target, logger=logger)
    logger.debug("Writing to file location: " + data_target['location'])


def _get_file_format(location):
    # ToDo
    format = "parquet"
    # return location.split('.')[-1]
    return format
