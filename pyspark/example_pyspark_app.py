# A sample PySpark Application

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, current_date


def main():
    """
    Main function
    """
    # create a spark session
    spark = (
        SparkSession.builder.appName("Sample PySpark").master("local[*]").getOrCreate()
    )

    # spark version
    print(">>>> Spark Version: {}".format(spark.version))

    # turn off verbose logging
    spark.sparkContext.setLogLevel("ERROR")

    # create sample data
    data = [
        {"id": 1, "desc": "item1"},
        {"id": 2, "desc": "item2"},
        {"id": 3, "desc": "item3"},
        {"id": 4, "desc": "item4"},
        {"id": 5, "desc": "item5"},
    ]

    # create a dataframe
    df = spark.createDataFrame(data)

    # print data
    df.show(n=5)

    # print schema
    df.printSchema()

    # Perform simple transformation
    final_df = (
        df.withColumn("new_id", col("id") * 1)
        .withColumn("current_ts", current_timestamp())
        .withColumn("current_date", current_date())
    )
    final_df.show(truncate=False)

    # write final_df locally
    final_df.write.format("parquet").mode("overwrite").partitionBy("current_date").save(
        "resources/output/final_df"
    )

    # stop spark
    spark.stop()


if __name__ == "__main__":
    main()
