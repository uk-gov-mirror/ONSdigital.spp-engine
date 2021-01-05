from es_aws_functions import aws_functions, general_functions
from spp.aws.s3.write_to_s3 import write_pandasDf_to_s3, write_sparkDf_to_s3

current_module = "SPP Engine - Write"


def spark_write(df, data_target, counter,
                environment, run_id, survey, **kwargs):
    """
    Writes a Spark DataFrame to a file.
    :param df: Spark DataFrame
    :param location: File location
    :param kwargs: Other keyword arguments to pass to df.write.save()
    """
    tmp_path = ''
    if isinstance(counter, int) & (counter >= 1):
        tmp_path = "/data" + str(counter)
    data_target['location'] = data_target['location'] + tmp_path
    write_sparkDf_to_s3(df, data_target, environment, run_id, survey)
    _write_log(data_target['location'], environment, run_id,
               survey)

    return


def pandas_write(df, data_target, environment,
                 run_id, survey, **kwargs):
    """
    Writes a Pandas DataFrame to a file.
    :param df: Pandas DataFrame
    :param location: File location
    :param kwargs: Other keyword arguments to pass to df.to_{format}()
    """
    # import s3fs  # Leave this in to check optional dependency explicitly
    # return getattr(df, "to_{}".format(_get_file_format(location)))(location, **kwargs)
    write_pandasDf_to_s3(df, data_target, environment, run_id, survey)
    _write_log(data_target['location'], environment, run_id, survey)


def _get_file_format(location):
    # ToDo
    format = "parquet"
    # return location.split('.')[-1]
    return format


def _write_log(location, environment, run_id,
               survey):
    try:
        logger = general_functions.get_logger(survey, current_module,
                                              environment, run_id)
    except Exception as e:
        raise Exception("{}:Exception raised: {}".format(current_module, e))
    logger.info("Writing to file")
    logger.info(f"Location: {location}")
