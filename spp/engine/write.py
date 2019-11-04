import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def write_file(df, location, mode='append', partitions=None, spark=None):

    """
    Writes a DataFrame to a file.
    :param df: Spark/Pandas DataFrame
    :param location: File location
    :param mode: Write mode (append, overwrite...)
    :param partitions: Specify list of partitions, to be used with Spark
    :param spark: If not None, use Spark
    """

    file_format = location.split('.')[-1]  # cvs, json, parquet...
    logger.info(f"Writing to file")
    logger.info(f"Location: {location}")
    if spark:
        if partitions:
            df.write.partitionBy(*partitions).format(file_format).mode(mode).save(location)
        else:
            df.write.format(file_format).mode(mode).save(location)
    else:
        import s3fs  # Leave this in to check optional dependency explicitly
        mode = 'a' if mode == 'append' else mode  # Deals with Spark/Pandas differences for default
        return getattr(df, f'to_{file_format}')(location, index=False, mode=mode)
