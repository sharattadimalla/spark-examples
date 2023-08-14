from pyspark.sql import SparkSession


def get_session():
    """
    Returns a spark session object
    """
    # create a spark session
    spark = (
        SparkSession.builder.appName("PySpark Application")
        .master("local[*]")
        .getOrCreate()
    )
    return spark
