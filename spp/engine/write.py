import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def spark_write(df, location, mode='append', partitions=None):
    """
    Writes a Spark DataFrame to a file.
    :param df: Spark DataFrame
    :param location: File location
    :param mode: Write mode (append, overwrite...)
    :param partitions: Specify list of partitions, to be used with Spark
    """
    _write_log(location)
    if partitions:
        df.write.partitionBy(*partitions).format(_get_file_format(location)).mode(mode).save(location)
    else:
        df.write.format(_get_file_format(location)).mode(mode).save(location)


def pandas_write(df, location, mode='a'):
    """
    Writes a Pandas DataFrame to a file.
    :param df: Pandas DataFrame
    :param location: File location
    :param mode: Write mode (a, w...)
    """
    _write_log(location)
    import s3fs  # Leave this in to check optional dependency explicitly
    return getattr(df, f'to_{_get_file_format(location)}')(location, index=False, mode=mode)


def _get_file_format(location):
    return location.split('.')[-1]


def _write_log(location):
    logger.info(f"Writing to file")
    logger.info(f"Location: {location}")
