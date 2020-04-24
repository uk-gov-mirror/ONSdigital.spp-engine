import logging

from spp.aws.s3.write_to_s3 import write_pandasDf_to_s3, write_sparkDf_to_s3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def spark_write(df, data_target, counter, **kwargs):
    """
    Writes a Spark DataFrame to a file.
    :param df: Spark DataFrame
    :param location: File location
    :param kwargs: Other keyword arguments to pass to df.write.save()
    """
    # _write_log(location)
    tmp_path = ''
    if isinstance(counter, int) & (counter >= 1):
        tmp_path = "/data" + str(counter)
    data_target['location'] = data_target['location'] + tmp_path
    write_sparkDf_to_s3(df, data_target)

    return


def pandas_write(df, data_target, **kwargs):
    """
    Writes a Pandas DataFrame to a file.
    :param df: Pandas DataFrame
    :param location: File location
    :param kwargs: Other keyword arguments to pass to df.to_{format}()
    """
    # _write_log(location)
    # import s3fs  # Leave this in to check optional dependency explicitly
    # return getattr(df, "to_{}".format(_get_file_format(location)))(location, **kwargs)
    write_pandasDf_to_s3(df,data_target)

def _get_file_format(location):
    # ToDo
    format = "parquet"
    # return location.split('.')[-1]
    return format


def _write_log(location):
    logger.info(f"Writing to file")
    logger.info(f"Location: {location}")
