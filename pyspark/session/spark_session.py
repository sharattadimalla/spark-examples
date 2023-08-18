from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


def get_session(config: dict) -> SparkSession:
    """Setup spark session

    Returns:
        SparkSession: Returns a spark session object
    """

    spark_conf = SparkConf().setAppName("PySpark Application").setMaster("local[*]")

    if len(config.keys()) > 0:
        for key, value in config.items():
            spark_conf.set(key, value)
            print("Set Spark config {0} >> {1}".format(key, value))

    spark = (
        SparkSession.builder.config(conf=spark_conf).enableHiveSupport().getOrCreate()
    )

    return spark
