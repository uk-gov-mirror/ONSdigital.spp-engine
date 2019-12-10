from spp.utils.logging import Logger

LOG = Logger(__name__).get()


def spark_write(df, location, **kwargs):
    """
    Writes a Spark DataFrame to a file.
    :param df: Spark DataFrame
    :param location: File location
    :param kwargs: Other keyword arguments to pass to df.write.save()
    """
    _write_log(location)
    df.write.save(location, format=_get_file_format(location), **kwargs)


def pandas_write(df, location, **kwargs):
    """
    Writes a Pandas DataFrame to a file.
    :param df: Pandas DataFrame
    :param location: File location
    :param kwargs: Other keyword arguments to pass to df.to_{format}()
    """
    _write_log(location)
    import s3fs  # Leave this in to check optional dependency explicitly
    return getattr(df, "to_{}".format(_get_file_format(location)))(location, **kwargs)


def _get_file_format(location):
    return location.split('.')[-1]


def _write_log(location):
    LOG.info(f"Writing to file")
    LOG.info(f"Location: {location}")
